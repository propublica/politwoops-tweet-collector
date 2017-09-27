var system = require('system');

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

var Debug = false;

var page = new WebPage(),
    address, output, size;

var current_timeout = null;

var inject_polyfills = function (requestData) {
  if ((/^http[s]?:[/][/]/.test(requestData.url) == true) && (/.js($|[#?])/.test(requestData.url))) {
    ['es5-shim.js', 'es5-sham.js'].forEach(function(fil){
      var result = page.injectJs(fil);
      if (Debug === true) {
        console.log((result ? 'Successfully injected' : 'Failed to inject'), fil, 'for', requestData.url);
      }
    });
  }
};

if (system.args.length < 1 || system.args.length > 2) {
  console.log('Usage: rasterize.js URL filename');
  phantom.exit();
} else {
  console.log('RenderDelayTimeout:', RenderRetryDelayTimeout);

  window.setTimeout(function() {
    console.log("Timeout, giving up.");
    phantom.exit();
  }, OverallTimeout);

  address = system.args[0];
  output = system.args[1];

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

  page.onResourceRequested = inject_polyfills;

  page.onConsoleMessage = function (msg) {
    console.log('Console Message:', msg);
  };
  page.onResourceError = function (err) {
    console.log('Failed to load resource', err.url, 'because', err.errorString);
  };
  page.onLoadStarted = function () {
    if (current_timeout != null) {
      console.log('Previous rendering schedule canceled.');
      clearTimeout(current_timeout);
      current_timeout = null;
    }
  };
  page.onLoadFinished = function (status) {
    if (Debug === true) {
      console.log('Load finished with status', status);
    }
    page.viewportSize = { width: WindowWidth, height: WindowHeight };
    page.clipRect = { width: WindowWidth, height: WindowHeight };
    current_timeout = setTimeout(render_page_first_try, RenderDelayTimeout);
    console.log('Will render in ' + (RenderDelayTimeout / 1000) + ' secs');
  };
  page.open(address);
}
