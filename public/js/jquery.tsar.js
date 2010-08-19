(function ($) {
  jQuery.tsar = {
    options: {
      queries: [],
      lines: {show: true},
      selection: { mode: "x" },
      xaxis: { mode: "time" },
      overview: {
        lines: { lineWidth: 1 },
        shadowSize: 0,
        yaxis: { ticks: [], min: 0, autoscaleMargin: 0.1 },
        legend: { show: false },
      },
    },

    plot: function (placeholder, options) {
      var options = $.extend({}, this.options, options);
      var overviewopts = $.extend({}, options, this.options.overview, options.overview);

      var plotplaceholder = $(placeholder);
      var viewplaceholder = false;
      if (options.overview && options.overview.placeholder) {
        var viewplaceholder = $(options.overview.placeholder);
      };

      var seriesopts = {};
      function encodeid (subject, attribute, cf) {
        return [subject, attribute, cf].join("/");
      };
      $.each(options.queries, function (i, query) {
        var key = encodeid(query.subject, query.attribute, query.cf);
        query.options["id"] = i;
        seriesopts[key] = query.options;
        delete query.options;
      });

      function render (json) {
        var data = [];
        $.each(json, function (s, series) {
          for (var i = 0; i < series.length; ++i) {
            series[i][0] *= 1000;
          };
          var sid = seriesopts[s]["id"];
          data[sid] = $.extend({data: series}, {label: s}, seriesopts[s]);
        });

        var plot = $.plot(plotplaceholder, data, options);
        if (viewplaceholder) {
          var overview = $.plot(viewplaceholder, data, overviewopts);

          plotplaceholder.bind("plotselected", function (event, ranges) {
            plot = $.plot(plotplaceholder, data,
              $.extend(true, {}, options, {
                xaxis: { min: ranges.xaxis.from, max: ranges.xaxis.to },
                bars: { 
                  barWidth: 
                    10 * ((ranges.xaxis.from - ranges.xaxis.to)/plotplaceholder.width()),
                },
            }));
            overview.setSelection(ranges, true);
          });

          viewplaceholder.bind("plotselected", function (event, ranges) {
            plot.setSelection(ranges);
          });
        };
      };

      function paramify (k, v) {
        return "&" + encodeURIComponent(k) + "=" + encodeURIComponent(v);
      };

      function querify (queries) {
        var url = "";
        $.each(queries, function (i, query) {
          url += paramify("subject", query.subject);
          delete query.subject;
          $.each(query, function (k, v) { url += paramify(k, v); });
        });

        return url;
      };

      var url = options.service + "?_accept=application/json&callback=?&missing=skip";
      url += querify(options.queries);

      $.ajax({
        url: url,
        success: render,
        method: "GET",
        dataType: "jsonp",
      });

      return plotplaceholder;
    },
  };
})(jQuery);
