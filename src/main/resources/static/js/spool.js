/*$(document).ready(function () {

    $('input[type="radio"]').click(function () {
        if ($(this).attr("value") == "tabla") {
            $('#tab_content2').hide('slow').addClass("tab-pane fade");
            $('#directory-tab').hide('slow').attr("aria-expanded", "false");
            $('#tab_content3').show('slow').addClass("tab-pane fade active in");
            $('#table-tab').show('slow').addClass("active").attr("aria-expanded", "true");
        }
        if ($(this).attr("value") == "carpeta") {
            $('#tab_content2').show('slow').addClass("tab-pane fade active in");
            $('#directory-tab').show('slow').attr("aria-expanded", "true");
            $('#tab_content3').hide('slow').addClass("tab-pane fade");
            $('#table-tab').hide('slow').attr("aria-expanded", "false");
            $('#directory-tab').show('slow').addClass("active").attr("aria-expanded", "true");
        }
    });
    $('input[type="radio"]').trigger('click');  // trigger the event
});*/

$(document).ready(function () {

    $('input[type="radio"]').change(function () {


        if ($(this).attr("value") === "tabla" && this.checked) {
            $('#tab_content2').hide('slow').addClass("tab-pane fade");
            $('#directory-tab').hide('slow').attr("aria-expanded", "false");
            $('#tab_content3').show('slow').addClass("tab-pane fade active in");
            $('#table-tab').show('slow').addClass("active").attr("aria-expanded", "true");
            $("#isDirectory").checked(true);
        }

        if ($(this).attr("value") === "carpeta" && this.checked) {
            $('#tab_content2').show('slow').addClass("tab-pane fade active in");
            $('#directory-tab').show('slow').attr("aria-expanded", "true");
            $('#tab_content3').hide('slow').addClass("tab-pane fade");
            $('#table-tab').hide('slow').attr("aria-expanded", "false");
            $('#directory-tab').show('slow').addClass("active").attr("aria-expanded", "true");
            $("#isDirectory").checked(true);
        }

        //programar ahora
        if ($(this).attr("value") === "yes" && this.checked) {
            $('#minute').show('slow');
            $('#lblMinute').show('slow');
            $('#hour').show('slow');
            $('#lblHour').show('slow');
            $('#month').show('slow');
            $('#lblMonth').show('slow');
            $('#weekday').show('slow');
            $('#lblWeekday').show('slow');
            $('#monthday').show('slow');
            $('#lblMonthday').show('slow');
            $('#startDate').show('slow');
            $('#lblStartDate').show('slow');
            $('#endDate').show('slow');
            $('#lblEndDate').show('slow');
            //$("#isSchedule").checked(true);
        }
        if ($(this).attr("value") === "not" && this.checked) {
            $('#minute').hide('slow');
            $('#lblMinute').hide('slow');
            $('#hour').hide('slow');
            $('#lblHour').hide('slow');
            $('#month').hide('slow');
            $('#lblMonth').hide('slow');
            $('#weekday').hide('slow');
            $('#lblWeekday').hide('slow');
            $('#monthday').hide('slow');
            $('#lblMonthday').hide('slow');
            $('#startDate').hide('slow');
            $('#lblStartDate').hide('slow');
            $('#endDate').hide('slow');
            $('#lblEndDate').hide('slow');
            //$("#isSchedule").checked(true);
        }

        //type connection
        if ($(this).attr("value") === "SID" && this.checked) {
            $('#lblType').val('SID');
            $('#lblType').text('SID');
        }
        if ($(this).attr("value") === "SERVICE_NAME" && this.checked) {
            $('#lblType').val('SERVICE NAME');
            $('#lblType').text('SERVICE NAME');
        }

    });
    $('input[type="radio"]').trigger('change');// trigger the event
});

function loadWhites() {

    var query = $('#query').val();
    if (!query) {
        alert('No hay consulta ingresada.');
        return;
    }
    console.log(query);

    var separator = $('#separator_').val();
    if (!separator) {
        alert('No hay separador seleccionado.');
        return;
    } else if (separator == '|') {
        separator = '%7C';
    }
    console.log(separator);

    var connectionId = $('#connectionId').val();
    if (!connectionId) {
        alert('No hay conexión seleccionada.');
        return;
    }
    console.log(connectionId);

    $.ajax({
        url: "http://10.231.236.25:8095/auxiliar/whites/" + query.replace(";", "") + "/" + separator + "/" + connectionId,
        // url: "http://127.0.0.1:8090/auxiliar/whites/" + query.replace(";", "") + "/" + separator + "/" + connectionId,
        // url: "http://10.231.236.25:8091/auxiliar/whites/" + query.replace(";", "") + "/" + separator + "/" + connectionId,
        //IP Publica url: "http://34.236.133.209:8090/auxiliar/whites/" + query.replace(";", "") + "/" + separator + "/" + connectionId,
        //url: "http://172.16.8.133:8090/auxiliar/whites/" + query.replace(";", "") + "/" + separator + "/" + connectionId,

        success: function (data) {
            $('#queryWhites').val(data);
        },
        error: function (request, status, error) {
            $('#queryWhites').val(request.responseText);
        }
    });
}

function saveWhites() {
    var formatedQuery = $('#queryWhites').val();
    if (formatedQuery.length > 0) {
        $('#query').val(formatedQuery);
    }
}

//FORMATEO DE FECHAS
function formatDate() {
    var query = $('#query').val();
    if (!query) {
        alert('No hay consulta ingresada.');
        return query;
    }
    console.log(query);

    var connectionId = $('#connectionId').val();
    if (!connectionId) {
        alert('No hay conexión seleccionada.')
        return;
    }
    console.log(connectionId);


    $.ajax({
        //IP PUBLICA url: "http://34.236.133.209:8090/auxiliar/formatofecha/" + query.replace(";", "") + "/" + connectionId,
        url: "http://10.231.236.25:8095/auxiliar/formatofecha/" + query.replace(";", "") + "/" + connectionId,
        // url: "http://localhost:8091/auxiliar/formatofecha/" + query.replace(";", "") + "/" + connectionId,
        //url: "http://172.16.8.133:8090/auxiliar/formatofecha/" + query.replace(";", "") + "/" + connectionId,
        success: function (data) {
            $('#queryFormatDate').val(data);
        },
        error: function (request, status, error) {
            $('#queryFormatDate').val(request.responseText);
        }
    });

}

function saveFormatDate() {
    var formatedDate = $('#queryFormatDate').val();
    if (formatedDate.length > 0) {
        $('#query').val(formatedDate);
    }
}