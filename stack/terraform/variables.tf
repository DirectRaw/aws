variable "region" {
  default = "eu-central-1"
}

variable "environment" {
  default = "dev"
}


variable "common_tags" {
  default = {
    Owner       = "Ravil Nagmetov"
    Environment = "Dev"
  }
}
