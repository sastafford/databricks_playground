# Databricks Apps

## Shiny App - Hello World

A simple application built using [Shiny](https://shiny.posit.co/py/).  

### Create
```
databricks apps create shiny-hello-world
```

### Sync
```
databricks sync --watch . /Workspace/Users/<user_name>/shiny-hello-world
```

### Deploy
```
databricks apps deploy shiny-hello-world --source-code-path /Workspace/Users/<user_name>/shiny-hello-world
```

### Debug
```
https://app-url/logz
```

### Delete app
```
databricks apps delete shiny-hello-world
```

### Other Examples

https://github.com/databricks/app-templates

