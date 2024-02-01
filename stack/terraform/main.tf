terraform {
  backend "s3" {
    bucket = "raw-test-s3"
    key    = "lambda"
    region = "eu-central-1"
  }
}

provider "aws" {
  profile = "default"
  region  = var.region
}



resource "aws_lambda_function" "test_lambda" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "hello.zip"
  function_name = "hello"
  role          = "arn:aws:iam::792725927764:role/AWSLambdaExecutionRole"
  handler       = "hello.hello"

  runtime = "python3.9"
}
