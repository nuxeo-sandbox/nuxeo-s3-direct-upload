nuxeo-s3-direct-upload
===================

WARNING: This plugin is a Proof of Concept. 

## List of Features (details below)

- Web components and sever side plugin to enable direct upload to s3 from the webui


## Build

Assuming maven and node are correctly setup on your computer:

```
git clone 
mvn package
```

## Install

- Install the package on your instance. 
- Upload nuxeo-document-import-s3.html, nuxeo-dropzone-s3.html and nuxeo-uploader-s3-behavior.html in view designer
- Upload aws-sdk.min.js and spark-md5.min.js into view designer 
- Update the import statements in the html files
- Override the default import dialog

```
<nuxeo-slot-content name="S3DocumentCreatePopup" slot="CREATE_POPUP_PAGES">
  <template>
    <nuxeo-s3-document-import id="bulkCreation"
      name="import"
      parent="[[parent]]"
      target-path="{{parentPath}}"
      suggester-children="{{suggesterChildren}}"></nuxeo-s3-document-import>
  </template>
</nuxeo-slot-content>
```


## About Nuxeo

Nuxeo provides a modular, extensible Java-based [open source software platform for enterprise content management](http://www.nuxeo.com/en/products/ep) and packaged applications for [document management](http://www.nuxeo.com/en/products/document-management), [digital asset management](http://www.nuxeo.com/en/products/dam) and [case management](http://www.nuxeo.com/en/products/case-management). Designed by developers for developers, the Nuxeo platform offers a modern architecture, a powerful plug-in model and extensive packaging capabilities for building content applications.

More information at <http://www.nuxeo.com/>
