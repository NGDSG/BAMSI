
function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}


var RetryCell = Backgrid.Cell.extend({
    template: _.template('<button class="button-warning pure-button" disabled>RETRY</button>'),
    events: {
      "click": "retryRow"
    },
    retryRow: function (e) {
      console.log("Individual to be retried" + this.model.attributes.individual);
      e.preventDefault();
      // send GET request to status URL
       $.getJSON('/resubmit/'+this.model.attributes.individual, { nfs: "yes" },function (data) {
        //the model is returned via this.model
         //get the updated jobs list
        jobs.fetch({reset: true});


     });


    },
    render: function () {
      this.$el.html(this.template());
      this.delegateEvents();
      return this;
    }
     });

      var columns = [{
        name: "id",
        editable: false,

        //cell: Backgrid.IntegerCell.extend({
        //  orderSeparator: ''
        //})

         // Backgrid.Extension.SelectRowCell lets you select individual rows
        cell: "select-row",

        // Backgrid.Extension.SelectAllHeaderCell lets you select all the row on a page
        headerCell: "select-all"


      }, {
        name: "individual",
         editable: false,
        cell: "string"
      },{
        name: "filteredcount",
        label : "alignments (kept)",
         editable: false,
        cell: "string"
      },{
        name: "rowcount",
        label: 'alignments (total)',
         editable: false,
        cell: "string"
      }
          , {
        name: "subpop",
           editable: false,
        cell: "string"
      },  {
        name: "status",
        editable: false,
        cell: "string"
      }

      ];


      var jobsModel = Backbone.Model.extend({
       initialize: function() {
        Backbone.Model.prototype.initialize.apply(this, arguments);
        this.on("change", function(model, options) {
            console.log("Saving change");
            if (options && options.save === false)
                return;
            model.save();
        });
    }
    });

    var tracking_ID = getParameterByName('tracking');

    var Jobs = Backbone.PageableCollection.extend({
        model: jobsModel,
         mode: "client"
        ,
        initialize: function (options) {
        this.url = '/jobs?tracking='+ tracking_ID;
      }

      });
      var jobs = new Jobs();
      var grid = new Backgrid.Grid({
        columns: columns,
        collection: jobs
      });
      var paginator = new Backgrid.Extension.Paginator({
        collection: jobs
      });

      // Initialize a client-side filter to filter on the client
      // mode pageable collection's cache.
      var filter = new Backgrid.Extension.ClientSideFilter({
       collection: jobs,
       fields: ['subpop','individual','status']
      });

      // Render the filter
      $("#grid").before(filter.render().el);

      $("#grid").append(grid.render().$el);
      $("#paginator").append(paginator.render().$el);


      // Add some space to the filter and move it to the right
     $(filter.el).css({float: "right", margin: "20px"});

     jobs.fetch({reset: true});
