At present, we have transferred the installation process of the software required during the packaging process and the running process to the packaging of the base image. 

If you want to update some software, please go to the corresponding address to modify it and publish a new base image.

- matrixorigin/golang
  - Dockerfile: https://github.com/matrixorigin/ci-images/tree/main/golang/ubuntu
  - Build Workflow: https://github.com/matrixorigin/ci-images/actions/workflows/golang-image.yaml
- matrixorigin/ubuntu
  - Dockerfile: https://github.com/matrixorigin/ci-images/tree/main/ubuntu
  - Build Workflow: https://github.com/matrixorigin/ci-images/actions/workflows/ubuntu-image.yaml
