function parse_query_string(query) {
  var vars = query.split("&");
  var query_string = {};
  for (var i = 0; i < vars.length; i++) {
    var pair = vars[i].split("=");
    // If first entry with this name
    if (typeof query_string[pair[0]] === "undefined") {
      query_string[pair[0]] = decodeURIComponent(pair[1]);
      // If second entry with this name
    } else if (typeof query_string[pair[0]] === "string") {
      var arr = [query_string[pair[0]], decodeURIComponent(pair[1])];
      query_string[pair[0]] = arr;
      // If third or later entry with this name
    } else {
      query_string[pair[0]].push(decodeURIComponent(pair[1]));
    }
  }
  return query_string;
}
var query = window.location.search.substring(1);
var qs = parse_query_string(query);
var tracking = qs.tracking;

if (typeof tracking == "undefined") {
  document.getElementById("main-chart").style.display="none";
  document.getElementById("grid").style.display="none";
  document.getElementsByClassName("backgrid-filter form-search")[0].style.display="none";
  document.getElementById("buttons").style.display="none";
  document.getElementById("paginator").style.display="none";


}
else {
  document.getElementById("tracking-box").value=tracking

}

function select_tracking() {
    var tracking = document.getElementById("tracking-box").value;
    var current = window.location.hostname + ":" + window.location.port;
    window.location.href =  "http:" + "/dash?tracking=" + tracking;
};
