# Introduction

[Apache Cassandra](http://cassandra.apache.org/), an [PA/EL](https://en.wikipedia.org/wiki/PACELC_theorem) NoSQL data store is one of the candidates being evaluated for storing [genomic variation](http://www.ebi.ac.uk/eva/?Variant%20Browser) data. Due to its high performance write throughput, it is also being considered as a "staging" database to ingest variant information in parallel from across multiple samples in studies such as the [3000 Rice genomes project](http://www.ebi.ac.uk/eva/?eva-study=PRJEB13618). The data stored in the staging database will subsequently be fed into the [EVA-pipeline](https://github.com/EBIvariation/eva-pipeline/) for processing. 

This folder contains Ansible scripts to automatically install and configure Cassandra on an arbitrary number of nodes.
 
#Pre-requisites

1. Ansible control node - a VM/machine running CentOS 7 - Embassy cloud images can be found [here](https://extcloud06.ebi.ac.uk/dashboard/project/images). 
2. Cassandra nodes - A set of VMs/machines running CentOS 7.
3. Ensure that Python 2 version >= 2.7 is installed on both the Ansible control node and the Cassandra nodes.
4. Install Ansible 2.3.1 for CentOS 7 using the instructions [here](https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-ansible-on-centos-7).
5. Add the IP addresses of the Cassandra nodes to the Ansible hosts inventory file: **/etc/ansible/hosts**. An example is shown below:

```
[cassnodes]
192.168.0.40 #cassnode-8
192.168.0.41 #cassnode-7
192.168.0.38 #cassnode-6
192.168.0.35 #cassnode-5
192.168.0.37 #cassnode-4
192.168.0.29 #cassnode-3
192.168.0.23 #cassnode-2
192.168.0.18 #cassnode-1
```

# Project files
**cassandra-install.yml** - Ansible playbook to automatically set up Apache Cassandra on a set of CentOS nodes - Node names assumed to be denoted by "cassnodes" in the Ansible inventory file: /etc/ansible/hosts (see pre-requisites section above). The script is based on the installation instructions [here](https://www.howtoforge.com/tutorial/how-to-install-apache-cassandra-on-centos-7/) and the recommended configuration instructions [here](http://docs.datastax.com/en/landing_page/doc/landing_page/recommendedSettings.html).
 
**disable-cpufreq.sh** - Disable CPU frequency scaling in the Cassandra nodes (used by cassandra-install.yml).