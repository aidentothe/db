output "spark_master_public_ip" {
  description = "Public IP address of the Spark master"
  value       = aws_instance.spark_master.public_ip
}

output "spark_master_web_ui" {
  description = "URL for Spark Master Web UI"
  value       = "http://${aws_instance.spark_master.public_ip}:8080"
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage"
  value       = aws_s3_bucket.spark_data_bucket.bucket
}

output "ssh_command" {
  description = "SSH command to connect to Spark master"
  value       = "ssh -i ~/.ssh/id_rsa ubuntu@${aws_instance.spark_master.public_ip}"
}

output "spark_submit_example" {
  description = "Example spark-submit command for S3 data"
  value       = "spark-submit --conf spark.hadoop.fs.s3a.bucket=${aws_s3_bucket.spark_data_bucket.bucket} your_script.py"
}