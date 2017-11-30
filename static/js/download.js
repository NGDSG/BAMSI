/**
 * Download a list of files.
 * @author speedplane
 */
function download_files(files) {
  function download_next(i) {
    if (i >= files.length) {
      return;
    }
    var a = document.createElement('a');
    a.href = files[i].download;
    a.target = '_parent';
    // Use a.download if available, it prevents plugins from opening.
    if ('download' in a) {
      a.download = files[i].filename;
    }
    // Add a to the doc for click to work.
    (document.body || document.documentElement).appendChild(a);
    if (a.click) {
      a.click(); // The click method is supported by most browsers.
    } else {
      $(a).click(); // Backup using jquery
    }
    // Delete the temporary link.
    a.parentNode.removeChild(a);
    // Download the next file with a small timeout. The timeout is necessary
    // for IE, which will otherwise only download the first file.
    setTimeout(function() {
      download_next(i + 1);
    }, 500);
  }
  // Initiate the first download.
  download_next(0);
}


function getURLParameter(name) {
  return decodeURIComponent((new RegExp('[?|&]' + name + '=' + '([^&;]+?)(&|#|;|$)').exec(location.search) || [null, ''])[1].replace(/\+/g, '%20')) || null;
};


function download_selected() {

    var tracking = getURLParameter('tracking');
    var selectedIDs = "";

    $('#grid input:checked').each(function() {
            if($(this).parent().attr('class') == 'select-row-cell renderable') {
                var parent = $(this).parent();
                var sibs = parent.siblings();
                var selectedID = (sibs[0]).innerHTML;
                selectedIDs+=selectedID+",";
            };
        });

    if(selectedIDs !== "" && tracking !== ""){
    selectedIDs = selectedIDs.slice(0, -1);

    var body = {
      "tracking"  :  tracking,
      "ids"   :  selectedIDs};

    // send list to BAMSI server, get back list of download links and total size on disk
    $.getJSON('/download', data=body,  success=function (data) {
        url_list = data['URLs'];
        if (confirm("Do you want to download " + data['URLs'].length + " files of total size " + data['size'] + " bytes?")) {
            var testlist = [];
            for (i = 0; i < url_list.length; i++) {
                testlist.push(JSON.parse('{ "download":"' + url_list[i]+ '"}'));
            }
            download_files(testlist);
        }
        else {
            alert("Download cancelled.")
        }
    });
    }
    else {
        alert("No files selected.")
    }

};
