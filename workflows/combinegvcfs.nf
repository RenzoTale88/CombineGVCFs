/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


include { paramsSummaryMap       } from 'plugin/nf-schema'
include { GLNEXUS                } from '../modules/nf-core/glnexus/main'
include { BCFTOOLS_CONCAT_SORT   } from '../modules/nf-core/bcftools/concat/main'
include { BCFTOOLS_INDEX         } from '../modules/nf-core/bcftools/index/main'                                                                                                                           
include { BCFTOOLS_STATS         } from '../modules/nf-core/bcftools/stats/main'                                                                                                                           
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_combinegvcfs_pipeline'


process GENOME_INTERVALS {
    conda "bioconda::pysam:0.22.1"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/pysam:0.22.1--py39hcada746_0' :
        'quay.io/biocontainers/pysam:0.22.1--py39hcada746_0' }"

    input:
    path fasta_fn
    path fai_fn

    output:
    path "intervals_*.bed"

    script:
    """
    #!/usr/bin/env python
    import pysam
    import re

    fai = open("${fai_fn}")
    n = 1
    bedfile = open(f"intervals_{n}.bed", "w")
    target_size = ${params.chunk_size}
    proc_size = 0
    tmp_list = []
    for line in fai:
        seq_id, seq_len, os1, os2, os3 = line.strip().split()
        seq_len = int(seq_len)
        proc_size += seq_len
        tmp_list.append(f'{seq_id}\\t0\\t{seq_len}\\n')
        if proc_size > target_size:
            for line in tmp_list:
                bedfile.write(line)
            proc_size = 0
            tmp_list = []
            n += 1
            bedfile.close()
            bedfile = open(f"intervals_{n}.bed", "w")
    if len(tmp_list) > 0:
        for line in tmp_list:
            bedfile.write(line)
    """
}


process fasta_index {
    conda "bioconda::samtools:1.21--h50ea8bc_0"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/samtools:1.21--h50ea8bc_0' :
        'quay.io/biocontainers/samtools:1.21--h50ea8bc_0' }"

    input:
    path fasta_fn

    output:
    path "${fasta_fn}.fai"

    script:
    """
    samtools faidx ${fasta_fn}
    """
}

process split_gvcf {
    label "process_single"
    conda "bioconda::bcftools:1.21--h8b25389_0"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/bcftools:1.21--h8b25389_0' :
        'quay.io/biocontainers/bcftools:1.21--h8b25389_0' }"

    input:
    tuple val(meta), path(bed), path(gvcf), path(tbi)

    output:
    tuple val(meta), path("${gvcf.simpleName}.${meta.id}.g.vcf.gz")

    script:
    """
    bcftools view -R ${bed} -O z ${gvcf} > ${gvcf.simpleName}.${meta.id}.g.vcf.gz
    """
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow COMBINEGVCFS {

    take:
    ch_samplesheet // channel: samplesheet read in from --input
    ch_fasta       // channel: fasta file from --fasta

    main:

    ch_versions = Channel.empty()
    
    // Create intervals to process
    ch_fai = fasta_index(ch_fasta)
    ch_intervals = GENOME_INTERVALS(ch_fasta, ch_fai)
    | flatten
    | map {
        fname ->
        def meta = [:]
        meta.id = fname.simpleName.replace('intervals_', '') as int
        [meta, fname]
    }

    // Split each GVCF by intervals to reduce database loading times
    ch_input = ch_intervals
    | combine(ch_samplesheet)
    | map {
        meta, bed, _meta2, gvcf, gtbi ->
        [meta, bed, gvcf, gtbi]
    }
    | split_gvcf

    // Collect the input VCFs to process
    // Run GLNEXUS on each dataset
    ch_bcf_single = ch_intervals
    | combine(
        ch_input | groupTuple(by: 0),
        by:0
    )
    | GLNEXUS

    // Index individual BCF files
    ch_tbi_single = BCFTOOLS_INDEX(ch_bcf_single.bcf)

    // Concat-sort the BCF files
    ch_vcf = ch_bcf_single.bcf
    | combine(ch_tbi_single.csi, by:0)
    | map {
        _meta, vcf, tbi ->
        def new_meta = [:]
        new_meta.id = "joint_calling"
        [new_meta, vcf, tbi]
    }
    | groupTuple(by: 0)
    | BCFTOOLS_CONCAT_SORT

    // Collect stats
    BCFTOOLS_STATS(
        ch_vcf.vcf | combine(ch_vcf.tbi, by: 0),
        [[],[]],
        [[],[]],
        [[],[]],
        [[],[]],
        [[],[]]
    )

    //
    // Collate and save software versions
    //
    softwareVersionsToYAML(ch_versions)
        .collectFile(
            storeDir: "${params.outdir}/pipeline_info",
            name: 'nf_core_'  + 'pipeline_software_' +  ''  + 'versions.yml',
            sort: true,
            newLine: true
        ).set { ch_collated_versions }


    emit:
    versions       = ch_versions                 // channel: [ path(versions.yml) ]

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
