# How to contribute

Elastic is an open-source project and we are looking forward to each
contribution.

Notice that while the [official Opensearch documentation](https://www.opensearch.co/guide/en/opensearchsearch/reference/current/index.html) is rather good, it is a high-level
overview of the features of Opensearch. However, Elastic tries to resemble
the Java API of Opensearch which you can find [on GitHub](https://github.com/opensearch/opensearchsearch).

This explains why you might think that some options are strange or missing
in Elastic, while often they're just different. Please check the Java API first.

Having said that: Opensearch is moving fast and it might be very likely
that we missed some features or changes. Feel free to change that.

## Your Pull Request

To make it easy to review and understand your changes, please keep the
following things in mind before submitting your pull request:

* You compared the existing implementation with the Java API, did you?
* Please work on the latest possible state of `olivere/opensearch`.
  Use `release-branch.v2` for targeting Opensearch 1.x and
  `release-branch.v3` for targeting 2.x.
* Create a branch dedicated to your change.
* If possible, write a test case which confirms your change.
* Make sure your changes and your tests work with all recent versions of
  Opensearch. We currently support Opensearch 1.7.x in the
  release-branch.v2 and Opensearch 2.x in the release-branch.v3.
* Test your changes before creating a pull request (`go test ./...`).
* Don't mix several features or bug fixes in one pull request.
* Create a meaningful commit message.
* Explain your change, e.g. provide a link to the issue you are fixing and
  probably a link to the Opensearch documentation and/or source code.
* Format your source with `go fmt`.

## Additional Resources

* [GitHub documentation](https://help.github.com/)
* [GitHub pull request documentation](https://help.github.com/en/articles/creating-a-pull-request)


## Develop

### Run all CI workflow

```bash
dagger call --src . ci
```

### Lint code

```bash
dagger call --src . lint
```

### Test code

```bash
dagger call --src . test
```

### Debug tests

Create file `.vscode/launch.json`.
```json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Connect to server",
            "type": "go",
            "request": "attach",
            "mode": "remote",
            "remotePath": "/src",
            "port": 4000,
            "host": "127.0.0.1"
        }
    ]
}
```

Run
```bash
dagger call --src . debug-test --run TestNAME up
```

To finish, Run de debug on Vsocode