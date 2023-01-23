resource "aws_secretsmanager_secret" "apikeys" {
  name = "${var.apikeys_name}"
}

resource "aws_secretsmanager_secret_version" "apikeys" {
  secret_id     = aws_secretsmanager_secret.apikeys.id
  secret_string = jsonencode(var.apikeys)
}
