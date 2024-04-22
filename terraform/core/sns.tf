# dataset-notification
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

# cdc-notification
resource "aws_sns_topic" "cdc_notification" {
  name = "${var.environment_name}-cdc-notification.fifo"
  fifo_topic                  = true
  content_based_deduplication = false
}

resource "aws_sns_topic_policy" "cdc_notification" {
  arn = aws_sns_topic.cdc_notification.arn

  policy = data.aws_iam_policy_document.cdc_notification_policy.json
}

data "aws_iam_policy_document" "cdc_notification_policy" {
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
      "${aws_sns_topic.cdc_notification.arn}",
    ]

    sid = "__default_statement_ID"
  }
}