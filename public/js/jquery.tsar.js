(function ($) {
  jQuery.tsar = {
    options: {
      lines: {show: true},
      selection: { mode: "x" },
      xaxis: { mode: "time" },
      yaxis: { units: false },
      plot: { show: true },
      grid: { 
        hoverable: true,
        mouseActiveRadius: 3,
      },
      legend: {
        show: true,
      },
      overview: {
        show: true,
        lines: { lineWidth: 1 },
        selection: { mode: "x" },
        shadowSize: 0,
        yaxis: { ticks: [], min: 0, autoscaleMargin: 0.1 },
        xaxis: { mode: "time" },
        legend: { show: false },
      },
      hooks: {
        processOptions: function (plot, options) {
          // XXX: Might want to handle more than 2 Y axes...
          var axes = ["yaxis", "y2axis"];
          for (var i in axes) {
            var axisname = axes[i],
              units = options[axisname].units;
            if (units) {
              var axis = plot.getAxes()[axisname],
                tickDecimals = axis.tickDecimals || 2,
                prefixes = "kMGTPEZY",
                sipow = function (i) { return Math.pow(10, (3 * i)) };
              axis.units = units;

              function sifactor (axis) {
                var limit = axis.datamax || axis.max;
                for (var i = prefixes.length, f = sipow(i); 
                  i >= 0 && (limit/f) < 2; i--, f = sipow(i + 1));
                return {value: f, suffix: prefixes[i] + axis.units};
              }
              if (!options[axisname].tickFormatter) {
                options[axisname].tickFormatter = function (v, axis) {
                  factor = sifactor(axis);
                  v /= factor.value;
                  v = v.toFixed(tickDecimals);
                  return v + " " + factor.suffix;
                }
              }
            }
          }
        },
      },
    },

    si: {
      prefixes: "kMGTPEZY",
      pow: function (i) { return Math.pow(2, (i + 1) * 10); },
      formatter: function (units) {
        var prefixes = this.prefixes,
          sipow = this.pow;
        function tickformatter (val, axis) {
          for (var i = prefixes.length, f = sipow(i); 
            i >= 0 && (axis.max/f) < 2; i--, f = sipow(i));
          val /= f;
          console.log(axis.tickSize);
          return val.toFixed(axis.tickDecimals) + " " + prefixes[i] + units;
        }
        return tickformatter;
      },
    },

    plot: function (container, opts) {
      var options = $.extend(true, {}, this.options, opts);

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

          overviewelem.bind("plotselected", function (event, ranges) {
            plot.setSelection(ranges);
          });
          overviewelem.bind("plotunselected", function () {
            var xaxis = plot.getAxes().xaxis;
            plot.setSelection({ xaxis: { from: xaxis.datamin, to: xaxis.datamax }});
            overview.clearSelection(true);
          });
        };
        plotelem.bind("plotselected", function (event, ranges) {
          plot = $.plot(plotelem, data,
            $.extend(true, {}, options, {
              xaxis: { min: ranges.xaxis.from, max: ranges.xaxis.to },
              bars: { 
                barWidth: 
                  10 * ((ranges.xaxis.from - ranges.xaxis.to)/plotelem.width()),
              },
          }));
          if (overviewelem) {
            overview.setSelection(ranges, true);
          }
        });

        var lastpt = null;
        var tooltip = "tsar-tooltip-" + rootid;
        function destroytip () {
          $("#" + tooltip).remove();
        }
        plotelem.bind("plothover", function (event, pos, item) {
          if (item) {
            if (lastpt != item.datapoint) {
              if (!lastpt || 
                  Math.abs(lastpt.pageX - item.pageX) ||
                  Math.abs(lastpt.pageY - item.pageY)) {
                lastpt = item.datapoint;
                destroytip();

                var x = item.datapoint[0].toFixed(2),
                    y = item.datapoint[1].toFixed(2);
                var content = item.series.label + ' (y=' + y + ')';
                $('<div id="' + tooltip + '" class="tsar-tooltip">' + 
                  content + '</div>').css({
                    position: "absolute",
                    display: "none",
                    top: item.pageY + 5,
                    left: item.pageX + 5,
                }).appendTo("body").fadeIn(200);
              }
            }
          } else {
            destroytip();
            lastpt = null;
          }
        });
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
