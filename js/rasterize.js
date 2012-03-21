var page = new WebPage(),
    address, output, size;

if (phantom.args.length < 2 || phantom.args.length > 3) {
    console.log('Usage: rasterize.js URL filename');
    phantom.exit();
} else {
    address = phantom.args[0];
    output = phantom.args[1];
    page.viewportSize = { width: 1280, height: 1024 };
    page.clipRect = { width: 1280, height: 1024 };
    
    var retries = 0;
    var deathClock = false;

    // final death clock, 30 seconds max for everything
    window.setTimeout(function() {
        console.log("Timeout, giving up");
        phantom.exit();
    }, 30000);

    page.open(address, function (status) {
        if (status == 'success') {
            window.setTimeout(function () {
                if (page.render(output)) {
                    console.log("Rendered results, stopping");
                    phantom.exit();
                } else if (retries < 5) {
                    retries += 1;
                    console.log("Didn't render, assuming a redirect, waiting for a new callback (#" + retries + ")");
                    if (!deathClock) {
                        deathClock = true;
                        console.log("Setting death clock of 7 seconds, in case this isn't a redirect");
                        window.setTimeout(function() {
                            phantom.exit();
                        }, 7000);
                    }
                } else {
                    console.log("Too many retries (" + retries + "), giving up");
                    phantom.exit();
                }
            }, 200);
        } else {
            console.log('Unable to load the address!');
            window.setTimeout(function () {
                phantom.exit();
            }, 200);
        }
    });
}