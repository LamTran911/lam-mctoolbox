import os
from werkzeug.utils import secure_filename
from flask import Flask,flash,request,redirect,send_file,render_template, Response, url_for, render_template_string
import pandas as pd
import re
import zipfile
from io import BytesIO
import requests
import json
from multiprocessing.dummy import Pool as ThreadPool
import time
from rq import Queue
from rq.job import Job
from worker import conn


timeout = 3600
q = Queue(connection=conn, default_timeout=timeout)

#app = Flask(__name__)
app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['client_id'] = 'e5c8bd0818567133d50b23855506bc7a'
app.config['client_secret'] = 'fb46f781268bbe82e900c9e6f374424d'
app.config['TEMPLATES_AUTO_RELOAD'] = True
tmp = None

def split_website_domain(row):
    if pd.isna(row['website']) == False:
        row['website'] = row['website'].lower().strip().replace(' ', '')
    nxt = row['website']
    try:
        if re.search('\/\/', nxt):
            nxt = nxt.split('//')[1]
        if re.search('www\.', nxt):
            nxt = nxt.split('www.')[1]
        if re.search('\/', nxt):
            nxt = nxt.split('/')[0]
    except:
        pass

    row['domain'] = nxt
    return row

# MATCHING
def matching(left, right):
  #------------------------INVALID PART-----------------------------------------
  invalid = pd.merge(left, right, 'left', left_on='domain', right_on='email_domain')
  mask = (invalid['email'].isnull() | invalid['website'].isnull() | invalid['company'].isnull())
  invalid = invalid[mask]
  invalid = invalid.dropna(subset=['company', 'website'], how = 'any')
  # invalid = invalid.drop_duplicates(['company'])
  # invalid = invalid.drop_duplicates(['website'])
  
  #--------------------------VALID PART--------------------------------------------------
  left = left.dropna(subset=['website', 'company'], how='any')
  # left = left.drop_duplicates(['domain'])
  # left = left.drop_duplicates(['company'])

  right = right.dropna()
  right = right.drop_duplicates(['email_domain'])

  valid = pd.merge(left, right, how = 'inner', left_on='domain', right_on='email_domain')
  valid = valid.dropna(subset=['website', 'domain', 'email', 'email_domain'])
  valid = valid.drop_duplicates(['domain'])
  valid = valid.drop_duplicates(['company'])
  valid = valid.drop_duplicates(['email_domain'])
  return valid, invalid

def data_preprocessing(df):
    try:
        df = df.drop_duplicates(subset = ['domain'])
        df = df.drop_duplicates(subset = ['company'])
        df = df.dropna(subset=['company', 'domain'], how='any')
    except:
        print("Error on data_preprocessing")
    return df

def data_postprocessing(df):
    try:
        df = df.drop_duplicates(subset = ['email'])
    except:
        print("Error on data_postprocessing")
    return df

def data_cleaning(df):
    mask = (df['email'] == 'Not Found')
    return df[~mask], df[mask]

# TÁCH EMAIL DOMAIN
def split_email_domain(row):
    if pd.isna(row['email']) == False:
        row['email'] = row['email'].lower().strip().replace(' ', '')
    nxt = row['email']
    try:
        if re.search('@', nxt):
            nxt = nxt.split('@')[1]
        if re.search('\/', nxt):
            nxt = nxt.split('/')[0]
    except:
        pass

    row['email_domain'] = nxt
    return row


def extract_domain(df):
    df = df.apply(split_website_domain, axis=1)
    return df


def match_domain(df):
    df = df.apply(split_website_domain, axis=1)
    df = df.apply(split_email_domain, axis=1)

    all_columns = set(df.columns)
    right_columns = set(['email', 'email_domain'])
    left_columns = (all_columns-right_columns)


    # CHIA RA LÀM 2 PHẦN
    left, right = df[list(left_columns)], df[list(right_columns)]
    valid, invalid = matching(left, right)
    return valid, invalid

def get_access_token():
  params = {
      'grant_type':'client_credentials',
      'client_id': app.config['client_id'],
      'client_secret': app.config['client_secret']
  }

  res = requests.post('https://api.snov.io/v1/oauth/access_token', data=params)
  resText = res.text.encode('ascii','ignore')

  return json.loads(resText)['access_token']

def get_domain_search(domain):
    try:
        token = get_access_token()
        params = {
            'access_token': token,
            'domain': domain,
            'type': 'generic',
            'limit': 1,
            'lastId': 0,
        }

        res = requests.get('https://api.snov.io/v2/domain-emails-with-info', params=params)

        js = json.loads(res.text)
        if js.get('success') == False:
            return 'unsuccess'
        if js.get('emails') == None or len(js.get('emails'))==0:
            return 'Not Found'

        return str(js['emails'][0]['email'])
    except:
        return 'unsuccess'



def get_email_count(domain):
  token = get_access_token()
  params = {'access_token':token,
          'domain':domain

  }

  res = requests.post('https://api.snov.io/v1/get-domain-emails-count', data=params)

  return json.loads(res.text)

def e2e(args):
    filename, filespec, df = args
    df = extract_domain(df)
    df = data_preprocessing(df)
    pool = ThreadPool(100)
    start_time = time.time()
    df['email'] = pool.map(get_domain_search, df['domain'])
    print("\n\n---Time executed {} data: {} seconds ---\n".format(len(df), (time.time() - start_time)))
    try: 
        del(df['domain'])
        unsuccess_id = (df['email'] == 'unsuccess')
        unsuccess_df = df[unsuccess_id]
        df = df[~unsuccess_id]
        valid, invalid = data_cleaning(df)
        valid = data_postprocessing(valid)
        valid['email'] = valid['email'].apply(lambda item: item.lower())
        print("Number of valid data: ", len(valid))
        print("Number of invalid data: ", len(invalid))
        print("Number of unsuccess data: ", len(unsuccess_df))
        # print('-----delay-----')
        # time.sleep(300)
        # print('-----Done-----')

        return filename, filespec, unsuccess_df, valid, invalid
    except:
        return filename, filespec, df
    # return True



# Upload API
@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            print('no file')
            return redirect(request.url)
        file = request.files['file']

        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            print('no filename')
            return redirect(request.url)
        else:
          try:
            filename = secure_filename(file.filename)
            print("User using " + filename)
            filetype = filename.split('.')[-1]
            filename = filename.split('.')[0]
            
            if filetype=='csv':
                df = pd.read_csv(file, sep=',', encoding="ISO-8859-1", engine='python')
            else:
                df = pd.read_excel(file, sheet_name=0)
           
            column_names = []
            print("Before: ", df.columns)
            for column in df.columns:
                name = column.lower().strip()
                if 'company' in name:
                    name = 'company'
                if 'website' in name:
                    name = 'website'
                if 'email' in name:
                    name = 'email'
                column_names += [name]
            df.columns = column_names
            print("After: ", df.columns)

            # ----------PROCESSING--------------------
            if request.form.get('split'):
                print("-----Splitting-----")
                n_rows = int(request.form['n_rows'])
                n = len(df)//n_rows
                if len(df) % n_rows:
                    n += 1
                with zipfile.ZipFile('output.zip', 'w') as zipf:
                    for i in range(n):
                        l = i * n_rows
                        r = min(l + n_rows, len(df))
                        output_df = df.iloc[l:r]
                        if request.form['filespecs'] == 'csv':
                          zipf.writestr(filename+'_' + str(i+1) + '.csv', output_df.to_csv(index=False, sep=';'))
                        else:
                          output_df.to_excel(filename+'_' + str(i+1) + '.xlsx', index=False)
                          zipf.write(filename+'_' + str(i+1) + '.xlsx', 
                                    filename+'_' + str(i+1) + '.xlsx')
                    zipf.close()
                return send_file('output.zip',
                      mimetype='zip',
                      as_attachment=True)   
            elif request.form.get('extract'):
                print("-----Extracting-----")
                df = extract_domain(df)
                df = data_preprocessing(df)
                if request.form['filespecs'] == 'xlsx':
                  df.to_excel(filename+'.xlsx', index=False)
                else:
                  df.to_csv(filename+'.csv', index=False, sep=';')

                return send_file(filename+'.'+request.form['filespecs'],
                       mimetype=request.form['filespecs'],
                       as_attachment=True)
            elif request.form.get('match'):
                print("-----Matching-----")
                valid, invalid = match_domain(df)
                del(valid['domain'])
                del(valid['email_domain'])
                del(invalid['domain'])
                del(invalid['email_domain'])
                # print(valid.head)
                with zipfile.ZipFile('output.zip', 'w') as zipf:
                  if request.form['filespecs'] == 'csv':
                    zipf.writestr(filename+'_valid.csv', valid.to_csv(index=False, sep=';'))
                    zipf.writestr(filename+'_invalid.csv', invalid.to_csv(index=False, sep=';'))
                  else:
                    valid.to_excel(filename+'_valid.xlsx', index=False)
                    invalid.to_excel(filename+'_invalid.xlsx', index=False)
                    zipf.write(filename+'_valid.xlsx', filename+'_valid.xlsx')
                    zipf.write(filename+'_invalid.xlsx', filename+'_invalid.xlsx')
                  zipf.close()

                return send_file('output.zip',
                       mimetype='zip',
                       as_attachment=True)
            else:
                print("-----E2E-----")
                inputs = (filename, request.form['filespecs'], df)
                job = q.enqueue(e2e, inputs, job_timeout=timeout)
                status = job.get_status()
                print(job.id, status)
                print("Hello")
                return redirect(url_for('result', id=job.id))
                       


          except Exception as err:
            print(err)
            return render_template("/error.html", e=err)
            # handle_exception(err)
            # return render_template(url_for('error', err=str(err)))


      #send file name as parameter to downlad

    return render_template('/index.html')

template_str='''<html>
    <head>
      {% if refresh %}
        <meta http-equiv="refresh" content="5">
      {% endif %}
    </head>
    <body>
    <p>Please don't close this tab until the process is finished, it may take a few minutes depending on the size of your data</p>
    <p>Output file will be automatically downloaded as soon as it is ready</p>
    <p>Response status: <strong>{{result}}</strong></p>
    </body>
    </html>'''


def get_template(data, refresh=False):
    return render_template_string(template_str, result=data, refresh=refresh)

@app.route('/result/<string:id>')
def result(id):
    job = Job.fetch(id, connection=conn)
    status = job.get_status()
    print("Hello2")
    print(job.id, status)
    if status == 'failed':
        job.cancel()
        return render_template("/error.html", e='Process failed')
    elif status == 'finished':
        try:
            filename, filespec, unsuccess_df, valid, invalid = job.result 
            job.cancel()

            with zipfile.ZipFile('output.zip', 'w') as zipf:
                if filespec == 'csv':
                    if len(unsuccess_df) > 0:
                        zipf.writestr(filename+'_unsuccess.csv', unsuccess_df.to_csv(index=False, sep=';'))
                    zipf.writestr(filename+'_valid.csv', valid.to_csv(index=False, sep=';'))
                    zipf.writestr(filename+'_invalid.csv', invalid.to_csv(index=False, sep=';'))
                else:
                    valid.to_excel(filename+'_valid.xlsx', index=False)
                    invalid.to_excel(filename+'_invalid.xlsx', index=False)
                    if len(unsuccess_df) > 0:
                        unsuccess_df.to_excel(filename+'_unsuccess.xlsx', index=False)
                        zipf.write(filename+'_unsuccess.xlsx', filename+'_unsuccess.xlsx')
                    zipf.write(filename+'_valid.xlsx', filename+'_valid.xlsx')
                    zipf.write(filename+'_invalid.xlsx', filename+'_invalid.xlsx')
                zipf.close()

            return send_file('output.zip',
                      mimetype='zip',
                      as_attachment=True) 
        except:
            filename, filespec, df = job.result 
            job.cancel()
            if filespec == 'xlsx':
                df.to_excel(filename+'.xlsx', index=False)
            else:
                df.to_csv(filename+'.csv', index=False, sep=';')

            return send_file(filename+'.'+filespec,
                       mimetype=filespec,
                       as_attachment=True)
                

          
    else:
        return get_template(status, refresh=True)
    # elif status in ['stopped', 'canceled']:
    #     return render_template("/index.html")



if __name__ == "__main__":
    print('running')
    app.run()
