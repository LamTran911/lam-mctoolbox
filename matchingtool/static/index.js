$('#fileup').change(function(){
//here we take the file extension and set an array of valid extensions
    var res=$('#fileup').val();
    var arr = res.split("\\");
    var filename=arr.slice(-1)[0];
    filextension=filename.split(".");
    filext="."+filextension.slice(-1)[0];
    valid=[".csv", ".xlsx"];
//if file is not valid we show the error icon, the red alert, and hide the submit button
    if (valid.indexOf(filext.toLowerCase())==-1){
        $( ".imgupload" ).hide("slow");
        $( ".imgupload.ok" ).hide("slow");
        $( ".imgupload.stop" ).show("slow");
      
        $('#namefile').css({"color":"red","font-weight":700});
        $('#namefile').html("File "+filename+" sai định dạng");

        $( "#submitbtn0" ).hide();
        $( "#fakebtn0" ).show();

       
        $( "#submitbtn1" ).hide();
        $( "#fakebtn1" ).show();

        $( "#submitbtn2" ).hide();
        $( "#fakebtn2" ).show();

        $( "#submitbtn3" ).hide();
        $( "#fakebtn3" ).show();
    }else{
        //if file is valid we show the green alert and show the valid submit
        $( ".imgupload" ).hide("slow");
        $( ".imgupload.stop" ).hide("slow");
        $( ".imgupload.ok" ).show("slow");
      
        $('#namefile').css({"color":"green","font-weight":700});
        $('#namefile').html(filename);

        $( "#submitbtn0" ).show();
        $( "#fakebtn0" ).hide();
      
        $( "#submitbtn1" ).show();
        $( "#fakebtn1" ).hide();

        $( "#submitbtn2" ).show();
        $( "#fakebtn2" ).hide();

        $( "#submitbtn3" ).show();
        $( "#fakebtn3" ).hide();

    }
});