function drawGraph(selector, measures) {
  var x_line = measures.map(function (m) { return Date.parse(m[0]); });
  x_line.unshift('x');

  var metrics = measures.map(function (m) { return m[2]; });
  metrics.unshift('Measure');

  var chart = c3.generate({
    bindto: selector,
    data: {
      x: 'x',
      columns: [
        x_line,
        metrics,
      ],
      types: {
        'Measure': 'bar',
      },
    },
    axis: {
      x: {
        type: 'timeseries',
        tick: {
          format: '%Y-%m-%d %H:%M:%S'
        }
      }
    },
    zoom: {
      enabled: true
    },
    padding: {
      left: 100,
      right: 100,
      top: 50,
    },
    size: {
      height: 600
    }
  });
}
