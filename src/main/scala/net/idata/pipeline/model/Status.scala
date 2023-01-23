package net.idata.pipeline.model

// Valid states
// - begin, processing, end

// Valid Codes
// - error
// - warning
// - info

case class Status(
                     processName: String,
                     publisherToken: String,
                     pipelineToken: String,
                     filename: String,
                     state: String,
                     code: String,
                     description: String
                 )