# Import necessary packages
import os
import io
import json
import pandas as pd
import hail as hl

# Get the Google billing project name and workspace name
billing_project = os.environ['WORKSPACE_NAMESPACE']
workspace = os.environ['WORKSPACE_NAME']
bucket = os.environ['WORKSPACE_BUCKET'] + "/"

# Verify that we've captured the environment variables
print("Billing project: " + billing_project)
print("Workspace: " + workspace)
print("Workspace storage bucket: " + bucket)
# List "entities" -- returns a JSON object listing and describing all data tables in the workspace
entities_list = fiss.fapi.list_entity_types(billing_project, workspace).text

# Display the data table descriptions (making the JSON more readable)
print(json.dumps(json.loads(entities_list), indent=4, sort_keys=True))
# List "entities" -- returns a JSON object listing and describing all data tables in the workspace
entities_list = fiss.fapi.list_entity_types(billing_project, workspace).text

# Display the data table descriptions (making the JSON more readable)
print(json.dumps(json.loads(entities_list), indent=4, sort_keys=True))

# load you gene dataset which must have the following information
# gene_symbol, Chromosome, Annotation Genomic Range Start, Annotation Genomic Range Stop
df = pd.read_excel(bucket + 'df.xlsx')

# initialize the hail
hl.init(default_reference="GRCh38", log='gnomAD-with-Hail-for-NDDs.log')

# loading the hail table data  of gnomAD into the environment
variants = hl.read_table("gs://gcp-public-data--gnomad/release/3.1.2/ht/genomes/gnomad.genomes.v3.1.2.sites.ht")

# Here we filter the hail table for only variants that are located inside the special range of that chromosome that we want.
# we store these variants in separate vcf file and we mention the name of the gene in the file name.
for i in range(len(df)):
    # filtering the variants
    sub = variants.filter((variants.locus.contig == df.loc[i, 'Chromosomes']) &
                          (variants.locus.position >= df.loc[i, 'Annotation Genomic Range Start']) &
                          (variants.locus.position <= df.loc[i, 'Annotation Genomic Range Stop']))

    # set the file name and the path inside terra storage
    a = str(i + 1) + '-' + df.loc[i, 'Gene_symbol'] + '.vcf'
    output_vcf_path = bucket + 'output/' + a

    # Export the table as a VCF file to the output VCF file path
    hl.export_vcf(sub, output_vcf_path)
    print("we have done till gene number :", i)
