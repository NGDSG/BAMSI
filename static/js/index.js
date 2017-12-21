//Template credit -  https://dribbble.com/shots/1821178-Sales-Report?list=buckets&offset=0


//Completed
var completedJobs = new ProgressBar.Circle('#completedJobs', {
  color: '#e81760',
  strokeWidth: 3,
  trailWidth: 3,
  duration: 1500,
  text: {
    value: '0%'
  },
  step: function(state, bar) {
    bar.setText((bar.value() * 100).toFixed(0) + "%");
  }
});
//Errored
var erroredJobs = new ProgressBar.Circle('#erroredJobs', {
  color: '#e88e3c',
  strokeWidth: 3,
  trailWidth: 3,
  duration: 1500,
  text: {
    value: '0%'
  },
  step: function(state, bar) {
    bar.setText((bar.value() * 100).toFixed(0) + "%");
  }
});
//Dispatched
var dispatchedJobs = new ProgressBar.Circle('#dispatchedJobs', {
  color: '#2bab51',
  strokeWidth: 3,
  trailWidth: 3,
  duration: 1500,
  text: {
    value: '0%'
  },
  step: function(state, bar) {
    bar.setText((bar.value() * 100).toFixed(0) + "%");
  }
});




function update_progress() {
  var tracking_ID = getParameterByName('tracking');

    // send GET request to status URL
    $.getJSON('/jobstatus?tracking='+tracking_ID, function (data) {
        // update UI completed
        percentCompleted = parseInt(data['COMPLETED'] / data['TOTAL']);
        //TODO Disable the error
        percentError = parseInt(data['ERROR']/ data['TOTAL']);
        percentSent = parseInt((data['DISPATCHED']  / data['TOTAL']));

        completedJobs.animate(percentCompleted);
        erroredJobs.animate(percentError);
        dispatchedJobs.animate(percentSent);


    });




}

$(document).ready(function() {





    // rerun in 2 seconds
    setTimeout(function () {
        update_progress();
    }, 2000);


});



