function changeOptions() {

    //Hacemos referencia a la vista, para obtener el valor de tableSize.

    var tableSize = $('#tableSize').val();
    if (!tableSize) {
        alert('No hay campos seleccionado.');
        return;
    }
    console.log(tableSize);

    $.ajax({
        // url: "http://localhost:8090/auxiliar/tablesize/" + tableSize,
        url: "http://10.231.236.25:8095/auxiliar/tablesize/" + tableSize,
        //url: "http://172.16.8.133:8090/auxiliar/tablesize/" + tableSize,

        success: function (data) {
            console.log(data);

            //Seteo los valores devueltos por el service, a los textbox fileSize,mappers.
            $('#fileSize').val(data.fetchsize);
            $('#mappers').val(data.mappers);
        },
        error: function (err) {
            alert(err);
        }
    });
}




