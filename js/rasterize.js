// OverallTimeout prevents the script from running forever.
var OverallTimeout = 15 * 1000;

// RenderDelayTimeout allows for redirects. Half a second should
// be sufficient. Two seconds is super-safe.
var RenderDelayTimeout = 2 * 1000;

// How long to delay before attempted to render again
// if the last-scheduled rendering fails.
var RenderRetryDelayTimeout = 2 * 1000;

// Output image size
var WindowWidth = 1280;
var WindowHeight = 1024;



var page = new WebPage(),
address, output, size;

var current_timeout = null;

if (phantom.args.length < 1 || phantom.args.length > 2) {
  console.log('Usage: rasterize.js URL filename');
  phantom.exit();
} else {
  console.log('RenderDelayTimeout:', RenderRetryDelayTimeout);

  window.setTimeout(function() {
    console.log("Timeout, giving up.");
    phantom.exit();
  }, OverallTimeout);

  address = phantom.args[0];
  output = phantom.args[1];

  var render_page = function () {
    var href = page.evaluate(function(){ return window.location.href; });
    var result = page.render(output);
    var success_or_failure = (result == true) ? 'success' : 'failure';
    console.log('Rendering', success_or_failure, 'for', href);
    return result;
  };
  var render_page_retry = function () {
    var result = render_page();
    if (result == false) {
      console.log('Final rendering attempt failed.');
    }
    phantom.exit();
  };
  var render_page_first_try = function () {
    var result = render_page();
    if (result == true) {
      phantom.exit();
    } else {
      setTimeout(render_page_retry, RenderRetryDelayTimeout);
    }
  };

  page.onConsoleMessage = function (msg) {
    console.log('Console Message:', msg);
  };
  page.onLoadStarted = function () {
    if (current_timeout != null) {
      console.log('Previous rendering schedule canceled.');
      clearTimeout(current_timeout);
    }
  };
  page.onLoadFinished = function (status) {
    page.viewportSize = { width: WindowWidth, height: WindowHeight };
    page.clipRect = { width: WindowWidth, height: WindowHeight };
    current_timeout = setTimeout(render_page_first_try, RenderDelayTimeout);
    console.log('Will render in ' + (RenderDelayTimeout / 1000) + ' secs');
  };
  page.open(address);
}

