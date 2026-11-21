output "vpc_id" {
  value       = aws_vpc.this.id
  description = "VPC ID."
}

output "private_subnet_ids" {
  value       = aws_subnet.private[*].id
  description = "Private subnet IDs across 3 AZs."
}

output "ecs_tasks_security_group_id" {
  value       = aws_security_group.ecs_tasks.id
  description = "Security group ID attached to ECS tasks."
}

output "execute_api_vpce_id" {
  value       = aws_vpc_endpoint.interface["execute-api"].id
  description = "VPC Interface Endpoint ID for private API Gateway (aws:SourceVpce)."
}
