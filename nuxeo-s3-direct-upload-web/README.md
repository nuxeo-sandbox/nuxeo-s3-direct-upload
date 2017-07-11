# Polymer App Toolbox - Starter Kit

[![Build Status](https://travis-ci.org/PolymerElements/polymer-starter-kit.svg?branch=master)](https://travis-ci.org/PolymerElements/polymer-starter-kit)

This template is a starting point for building apps using a drawer-based
layout. The layout is provided by `app-layout` elements.

This template, along with the `polymer-cli` toolchain, also demonstrates use
of the "PRPL pattern" This pattern allows fast first delivery and interaction with
the content at the initial route requested by the user, along with fast subsequent
navigation by pre-caching the remaining components required by the app and
progressively loading them on-demand as the user navigates through the app.

The PRPL pattern, in a nutshell:

* **Push** components required for the initial route
* **Render** initial route ASAP
* **Pre-cache** components for remaining routes
* **Lazy-load** and progressively upgrade next routes on-demand

## Nuxeo Stuff

An empty Nuxeo Bundle that is based on the Polymer Started Kit. Common `polymer-cli` commands has been wrapped in `NPM` run-script.

## Polymer Specific

### Start the development server

This command serves the app at `http://localhost:3000` and provides basic URL
routing for the app:

```shell
npm run serve
```

By default, a proxy is set between `http://localhost:3000/nuxeo` a `http://localhost:8080/nuxeo`, in order to share the HTTP Session between Nuxeo and your Polymer application. You can change the default value using:

```shell
npm run serve --proxy-path="nuxeo" --proxy-target="http://localhost:8080/nuxeo"
```

### Build

This command performs HTML, CSS, and JS minification on the application
dependencies, and generates a service-worker.js file with code to pre-cache the
dependencies based on the entrypoint and fragments specified in `polymer.json`.
The minified files are output to the `build/unbundled` folder, and are suitable
for serving from a HTTP/2+Push compatible server.

In addition the command also creates a fallback `build/bundled` folder,
generated using fragment bundling, suitable for serving from non
H2/push-compatible servers or to clients that do not support H2/Push.

    npm run build

### Preview the build

This command serves the minified version of the app at `http://localhost:3000`
in an unbundled state, as it would be served by a push-compatible server:

    npm run serve -- build/unbundled

This command serves the minified version of the app at `http://localhost:3000`
generated using fragment bundling:

    npm run serve -- build/bundled

### Run tests

This command will run [Web Component Tester](https://github.com/Polymer/web-component-tester)
against the browsers currently installed on your machine:

    npm run test

### Adding a new view

You can extend the app by adding more views that will be demand-loaded
e.g. based on the route, or to progressively render non-critical sections of the
application. Each new demand-loaded fragment should be added to the list of
`fragments` in the included `polymer.json` file. This will ensure those
components and their dependencies are added to the list of pre-cached components
and will be included in the `bundled` build.
