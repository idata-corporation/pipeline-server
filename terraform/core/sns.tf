resource "aws_sns_topic" "dataset_notification" {
  name = "${var.environment_name}-dataset-notification"
}

resource "aws_sns_topic_policy" "dataset_notification" {
  arn = aws_sns_topic.dataset_notification.arn

  policy = data.aws_iam_policy_document.dataset_notification_policy.json
}

data "aws_iam_policy_document" "dataset_notification_policy" {
  policy_id = "__default_policy_ID"

  statement {
    actions = [
      "SNS:Subscribe",
      "SNS:SetTopicAttributes",
      "SNS:RemovePermission",
      "SNS:Receive",
      "SNS:Publish",
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
      "SNS:DeleteTopic",
      "SNS:AddPermission",
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"

      values = [
        "${data.aws_caller_identity.current.account_id}",
      ]
    }

    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      "${aws_sns_topic.dataset_notification.arn}",
    ]

    sid = "__default_statement_ID"
  }
}

resource "aws_sns_topic_subscription" "notification" {
  topic_arn = aws_sns_topic.dataset_notification.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.notification.arn
  raw_message_delivery = true
  delivery_policy = jsonencode({
    healthyRetryPolicy = {
        numRetries = 3
        minDelayTarget = 20
        maxDelayTarget= 20
        numMinDelayRetries = 0
        numMaxDelayRetries = 0
        numNoDelayRetries = 0
        backoffFunction = "linear"
    }
  })
}