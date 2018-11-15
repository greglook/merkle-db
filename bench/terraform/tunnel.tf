# Use a terraform null-resource to run the final tunnel-setup script once the
# cluster is up.

resource "null_resource" "master_tunnel" {
  provisioner "local-exec" {
    command = "ansible-playbook -i inventory.ini -i '${aws_emr_cluster.benchmark.master_public_dns},' init-tunnel.yml --extra-vars 'cluster_master_host=${aws_emr_cluster.benchmark.master_public_dns}'"
    working_dir = "../ansible"
    environment = {
      ANSIBLE_FORCE_COLOR = true
      ANSIBLE_HOST_KEY_CHECKING = false
    }
  }
}
