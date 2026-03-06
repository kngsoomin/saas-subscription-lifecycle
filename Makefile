ANSIBLE_DIR=infra/ansible

bootstrap:
	cd $(ANSIBLE_DIR) && ansible-playbook playbooks/bootstrap.yml

deploy-airflow:
	cd $(ANSIBLE_DIR) && ansible-playbook playbooks/deploy-airflow.yml

rebuild-airflow:
	cd $(ANSIBLE_DIR) && ansible-playbook playbooks/bootstrap.yml -e force_redeploy=true
	cd $(ANSIBLE_DIR) && ansible-playbook playbooks/deploy-airflow.yml