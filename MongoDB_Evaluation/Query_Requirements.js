//Find SNP using chromosome and position
db.variant_chr21_1_1.find({"chr": "21", "start": 9411413, "ref": "T"});

//Find SNP using rsID
db.variant_chr21_1_1.find({"chr": "21", "ids.0": "rs181691356", "ref":"C"});

//Find INDEL using chromosome and position
db.variant_chr21_1_1.find({"chr": "21", "start": 10420607, type: "INDEL"});

//Find INDEL using rsID
db.variant_chr21_1_1.find({"chr": "21", "ids": "rs386816253", type: "INDEL"});

//Find cell line using chromosome and position
db.variant_chr21_1_1.find({"chr": "21", "start": 9411413, "ref": "T"}, {"files.samp":1});

//Find cell line using rsID
db.variant_chr21_1_1.find({"chr": "21", "ids.0": "rs181691356", "ref":"C"}, {"files.samp":1});

//List cell lines that are homozygous/heterozygous for allele X at position X on chromosome X


//Find cell lines with X to X mutation in ensembl ID ENSTXXXXXXXXXXX residue XXXX
db.variant_chr21_1_1.find({"chr": "21", "annot.ct.enst": "ENST00000459169", "ref": "G", "alt":"T"}, {"files.samp":1});

//Find cell lines with X to X mutation in multiple ensembl IDs and residues
db.variant_chr21_1_1.find({"chr": "21", "annot.ct.enst" : {$in: ["ENST00000459169", "ENST00000440782"]}, "ref": "G", "alt":"T"});

//List cell lines with mutation from X to X at rsXXXXXXXX
db.variant_chr21_1_1.find({"chr": "21", "ids": "rs181691356", "ref": "G", "alt":"T"}, {"files.samp":1});
