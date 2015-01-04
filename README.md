# af3ro - afero-compatible interface to S3

Will check etags when writing a file to avoid writing files that don't need to
be overwritten (etags are MD5 sums of file contents)

```
package foo

import (
    "github.com/goamz/goamz/aws"
    "github.com/ryansb/af3ro"
    "github.com/spf13/afero"
)


var s3fs afero.Fs = af3ro.NewS3Fs(af3ro.Bucket("some.bucket.name"), af3ro.Region(aws.USEast), af3ro.EnvAuth())
```

## Caveats

Don't use this for big files for these reasons:

* Files are *stored in memory* until being written to S3 so you can OOM your
  program.
* Multipart uploads aren't supported, full file contents are uploaded in one
  PUT request.
* Etags for multipart files aren't supported and will fail, causing files
  uploaded initially as multipart to *always* be re-uploaded.

Data is only written to S3 when a file is *closed* so be aware that failing to
close a file means it won't be written.

Permissions are translated from os.FileMode to AWS S3 ACLs, which are less
expressive and don't completely map to FileModes, so double-check that the
correct permissions are set in S3

Basically, this is for small files where you (for some reason) aren't able to
use the goamz/s3 library directly and have to present a local-like filesystem
interface.
