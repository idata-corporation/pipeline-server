variable "environment_name" {
  description = "Environment name for your Pipeline (e.g. mycompany-poc)"
  type        = string
  default     = "idata-poc"
}

variable "apikeys_name" {
  description = "Name for the api-keys that will be stored in Secrets Manager"
  default     = "idata-poc-apikeys"
}

variable "apikeys" {
  default = {
    defaultKey = "610ad41b-6fde-4831-8580-f4d7dc769640"
    extraKey = "1847626a-5b46-4d43-827c-25f323d9201b"
  }

  type = map(string)
}
