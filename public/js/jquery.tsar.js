(function ($) {
  jQuery.tsar = {
    options: {
      queries: [],
      lines: {show: true},
      selection: { mode: "x" },
      xaxis: { mode: "time" },
      plot: { show: true },
      legend: {
        show: true,
      },
      overview: {
        show: true,
        lines: { lineWidth: 1 },
        shadowSize: 0,
        yaxis: { ticks: [], min: 0, autoscaleMargin: 0.1 },
        legend: { show: false },
      },
    },

    plot: function (container, options) {
      var options = $.extend(true, {}, this.options, options);
      options.overview = $.extend({}, options, this.options.overview, options.overview);

      container = $(container);
      var rootid = container.attr("id");

      if (options.plot.container) {
        var plotelem = $(options.plot.container);
      } else {
        var plotelem = $('<div class="tsar-plot" id="tsar-plot-' + rootid + '">');
        container.append(plotelem);
      }

      var overviewelem = false;
      if (options.overview.container) {
        overviewelem = $(options.overview.container);
      } else if (options.overview.show) {
        overviewelem = $('<div class="tsar-overview" id="tsar-overview-' + rootid + '">');
        container.append(overviewelem);
      }

      var legendelem = false;
      if (options.legend.container) {
        var legendelem = $(options.legend.container);
      } else {
        var legendelem = $('<div class="tsar-legend" id="tsar-legend-' + rootid + '">');
        container.append(legendelem);
        options.legend.container = legendelem;
      }

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
          data[sid] = $.extend(true, {data: series}, {label: s}, seriesopts[s]);
        });

        var plot = $.plot(plotelem, data, options);
        if (overviewelem) {
          var overview = $.plot(overviewelem, data, options.overview);

          plotelem.bind("plotselected", function (event, ranges) {
            plot = $.plot(plotelem, data,
              $.extend(true, {}, options, {
                xaxis: { min: ranges.xaxis.from, max: ranges.xaxis.to },
                bars: { 
                  barWidth: 
                    10 * ((ranges.xaxis.from - ranges.xaxis.to)/plotelem.width()),
                },
            }));
            overview.setSelection(ranges, true);
          });

          overviewelem.bind("plotselected", function (event, ranges) {
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

      return container;
    },
  };
})(jQuery);
