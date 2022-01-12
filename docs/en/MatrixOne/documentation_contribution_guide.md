# How MatrixOne documentation project is managed?

The MatrixOne documentation is managed in 3 repos:

* The main project framework and CI settings are in the [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io) repo. 

* The markdown contents are in the docs/en/MatrixOne folder of [matrixone](https://github.com/matrixorigin/matrixone) repo. 

* The images and unstructured data are located in the [artwork](https://github.com/matrixorigin/artwork) repo. 

The `matrixorigin.io` repo contains a submodule which links to matrixone repo contents. The images are referred as webimage links from `artwork` repo. The `matrixorigin.io` has implemented a CI project, which will be triggered by a new code merge. This CI workflow publishs the documentation to https://docs.matrixorigin.io/.

The documentation project is based on the [mkdocs-material](https://github.com/squidfunk/mkdocs-material). You can find the corresponding syntax and commands with this project.

# How MatrixOne documentation content is organized?

The MatrixOne documentation content is planned with 6 main modules.  

* Overview: MatrixOne's introduction, features, architecture, tech design etc. 

* Get Started: How to quickly deploy and run a MatrixOne in a standalone or a distributed environment.

* Develop: How to develop some applications based on MatrixOne using different programming language.

* Troubleshoot: Introduce common errors and debugging tools.

* Reference: SQL reference, Configuration parameters, Error Codes.

* FAQs: Product, Technical Design,SQL, Deployment FAQs.

# How to contribute to documentation project?

1. Fork [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io) and [matrixone](https://github.com/matrixorigin/matrixone) repos.
2. Clone the [matrixorigin.io](https://github.com/matrixorigin/matrixorigin.io) repo, using `--repository` to retrieve the submodule of [matrixone](https://github.com/matrixorigin/matrixone) repo.
```
$ git clone --recursive git@github.com:yourusername/matrixorigin.io.git
```
3. As the local repo has the full documentation project code, you can run `mkdocs serve` under `matrixorigin.io` folder, and check `http://localhost:8000` to check if this project runs well.  
```
$ mkdocs serve
```
4. Make your modification. If the modification is about the project settings, updating sitemap with new pages, or updating the CI workflow code. You can always check `http://localhost:8000` to see if your modification is effective. If your modification is about the markdown contents, after you update the `docs` submodule of `matrixorigin.io`, the same modifcation should be applied to the `matrixone` repo.

5. Push your git commits to your Github repos and make PR to `matrixorigin.io` and `matrixone` repos. 

6. Once your code for both repos is merged, the CI workflow starts running until the documentation website being updated.

# What we are expecting?

Anyone using MatrixOne or interested in MatrixOne is welcome to make contributions for the documentation. You can report confusing concepts, wrong descriptions or better organization format. 

For the 0.2.0 version, our SQLs and configuration settings are not fully tested, if you find contraints or errors, you are welcome to make your modifiction. 

The FAQs part is a onging project too, the ones who are more familiar with the project can contribute better answers. 