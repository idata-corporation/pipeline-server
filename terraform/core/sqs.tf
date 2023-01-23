# file-notifier
resource "aws_sqs_queue" "file_notifier" {
  name = "${var.environment_name}-file-notifier"

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.file_notifier_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    name = var.environment_name
  }
}

resource "aws_sqs_queue" "file_notifier_dlq" {
  name = "${var.environment_name}-file-notifier-dlq"
  tags = {
    name = var.environment_name
  }
}

resource "aws_sqs_queue_policy" "file_notifier_policy" {
  queue_url = aws_sqs_queue.file_notifier.id

  policy = data.aws_iam_policy_document.file_notifier_policy_document.json
}

data "aws_iam_policy_document" "file_notifier_policy_document" {
  statement {
    actions   = ["SQS:*"]
    resources = ["${aws_sqs_queue.file_notifier.arn}"]
    effect    = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_notification" "raw_notification" {
  bucket = "${var.environment_name}-raw"

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".metadata.json"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.csv"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.json"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.xml"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.orc"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.parquet"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.gz"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.jar"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.tar"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.xls"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.xlsx"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.zip"
  }

  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.png"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.jpeg"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.gif"
  }
  queue {
    queue_arn     = aws_sqs_queue.file_notifier.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".dataset.pdf"
  }
}

# notification
resource "aws_sqs_queue" "notification" {
  name = "${var.environment_name}-notification"

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.notification_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    name = var.environment_name
  }
}

resource "aws_sqs_queue" "notification_dlq" {
  name = "${var.environment_name}-notification-dlq"
  tags = {
    name = var.environment_name
  }
}

resource "aws_sqs_queue_policy" "notification_policy" {
  queue_url = aws_sqs_queue.notification.id

  policy = data.aws_iam_policy_document.notification_policy_document.json
}

data "aws_iam_policy_document" "notification_policy_document" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = ["${aws_sqs_queue.notification.arn}"]
    effect    = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    condition {
      test     = "ForAnyValue:ArnEquals"
      variable = "aws:SourceArn"
      values   = ["${aws_sns_topic.dataset_notification.arn}"]
    }
  }
}
