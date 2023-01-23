resource "aws_iam_user" "snowflake" {
  name = "${var.environment_name}_snowflake"
  path = "/system/"

  tags = {
    tag-key = "tag-value"
  }
}

resource "aws_iam_access_key" "snowflake" {
  user = aws_iam_user.snowflake.name
}

resource "aws_iam_user_policy" "policy" {
  name        = "${var.environment_name}_snowflake_access"
  user        = aws_iam_user.snowflake.name

  policy = jsonencode(
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:GetObject",
              "s3:GetObjectVersion"
            ],
            "Resource": "arn:aws:s3:::${var.environment_name}-temp/snowflake/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::${var.environment_name}-temp",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "snowflake/*"
                    ]
                }
            }
        }
    ]
  }
  )
}