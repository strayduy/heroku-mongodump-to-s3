heroku-mongodump-to-s3
======================

Heroku Config
-------------

Buildpacks
* https://github.com/heroku/heroku-buildpack-python.git
* https://github.com/strayduy/heroku-buildpack-mongodb.git

```
# Use the multi-buildpack
heroku config:set BUILDPACK_URL=https://github.com/ddollar/heroku-buildpack-multi.git

# Manually append mongodb bin directory to PATH (https://github.com/ddollar/heroku-buildpack-multi/issues/5)
heroku config:set PATH=/app/.heroku/python/bin:/usr/local/bin:/usr/bin:/bin:/app/vendor/mongodb/bin
```

Note: We have to manually set the PATH because [the multi-buildpack doesn't allow the individual buildpacks to update the PATH](https://github.com/ddollar/heroku-buildpack-multi/issues/5).
