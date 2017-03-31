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
//NOTE: This query only works after performing the update to flip the "def" key into a value in the "samp" sub-document
//See https://github.com/EBIvariation/eva-variant-warehouse-research/blob/master/MongoDB_Evaluation/NewMongoDesign/MongoDesignUpdate.py
db.variants_hsap_87_87_mod.find({"chr": "21", "start": 9541066, "ref": "T", "files.samp.0|1": {$exists: true}}, 
									  {"files": {$elemMatch: {"samp.0|1": {$exists: true}}}, "files.samp.0|1": 1});

//Return the allele frequency for allele X at position X on chromosome X
//Based on Josemi's proposed changes to statistics collection, 
//this may be possible with a bit of a change in the web-service code

//Find cell lines with X to X mutation in ensembl ID ENSTXXXXXXXXXXX residue XXXX
db.variant_chr21_1_1.find({"chr": "21", "annot.ct.enst": "ENST00000459169", "ref": "G", "alt":"T"}, {"files.samp":1});

//Find cell lines with X to X mutation in multiple ensembl IDs and residues
db.variant_chr21_1_1.find({"chr": "21", "annot.ct.enst" : {$in: ["ENST00000459169", "ENST00000440782"]}, "ref": "G", "alt":"T"});

//List cell lines with mutation from X to X at rsXXXXXXXX
db.variant_chr21_1_1.find({"chr": "21", "ids": "rs181691356", "ref": "G", "alt":"T"}, {"files.samp":1});

//Return all samples with a homozygous/heterozygous reference for allele X at position X on chromosome X
db.variant_chr21_1_1_sample_mod.find({"chr": "21", "start": 9411413, "ref": "T", "files.samp.0|0": {$exists: true}}, {"files.samp.0|0":1})

//List Variants in a given region and only those that are homozygous in sample X
var numericSampleIndex = db.files_1_1.findOne( {"fid" : "8605" }, {"samp.HG00116":1})["samp"]["HG00116"]

db.variant_chr21_1_1_sample_mod.ensureIndex({"files.samp.0|0":1}, {sparse: true});
db.variant_chr21_1_1_sample_mod.ensureIndex({"files.samp.0|0.s":1}, {sparse: true});
db.variant_chr21_1_1_sample_mod.ensureIndex({"files.samp.0|0.e":1}, {sparse: true});
db.variant_chr21_1_1_sample_mod.find({$and: [{"chr":"21", "start": {$gte: 10997575}, "end": {$lte: 18639593}}, 
  {$or: [{"files.samp.0|0": numericSampleIndex},
	  	{"files": {$elemMatch: {"samp.0|0": {$elemMatch: {s: {$lte: numericSampleIndex}, e: {$gte:numericSampleIndex}}} , fid: "8605"}}}
	  	]
  }
]});


db.variant_chr21_1_1_sample_mod.find({$and: [{"chr": "21"}, {"start": 9411413}, {"ref": "T"}, 
  {$or: [{"files.samp.0|0": numericSampleIndex},
	  	{"files": {$elemMatch: {"samp.0|0": {$elemMatch: {s: {$lte: numericSampleIndex}, e: {$gte:numericSampleIndex}}} , fid: fileID}}}
	  	]
  }
]})

