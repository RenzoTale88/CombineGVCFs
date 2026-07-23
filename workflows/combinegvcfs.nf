/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


include { paramsSummaryMap       } from 'plugin/nf-schema'
include { GLNEXUS                } from '../modules/nf-core/glnexus/main'
include { BCFTOOLS_INDEX         } from '../modules/nf-core/bcftools/index/main'
include { BCFTOOLS_STATS         } from '../modules/nf-core/bcftools/stats/main'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_combinegvcfs_pipeline'


process GENOME_INTERVALS {
    conda "bioconda::pysam=0.22.1"
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


process SAMTOOLS_FAIDX {
    conda "bioconda::samtools=1.21"
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

// Sorted concatenation of BCFs
process BCFTOOLS_CONCAT_SORT {
    tag "$meta.id"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/bcftools:1.20--h8b25389_0':
        'biocontainers/bcftools:1.20--h8b25389_0' }"

    input:
    tuple val(meta), path(vcfs), path(tbi)

    output:
    tuple val(meta), path("${prefix}.vcf.gz")    , emit: vcf
    tuple val(meta), path("${prefix}.vcf.gz.tbi"), emit: tbi, optional: true
    tuple val(meta), path("${prefix}.vcf.gz.csi"), emit: csi, optional: true
    path  "versions.yml"                         , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args   ?: ''
    prefix   = task.ext.prefix ?: "${meta.id}"
    def tbi_names = tbi.findAll { file -> !(file instanceof List) }.collect { file -> file.name }
    def create_input_index = vcfs.collect {
        vcf ->
        tbi_names.contains(vcf.name + ".tbi") || tbi_names.contains(vcf.name + ".csi") ? "" : "bcftools index ${vcf}"
    }.join("\n    ")
    """
    ${create_input_index}

    # Inputs are passed as sorted using their index number, avoiding the need for concat/sorting
    bcftools concat \\
        $args \\
        -O z \\
        -a \\
        --threads $task.cpus \\
        --write-index=tbi \\
        --output ${prefix}.vcf.gz \\
        \$( ls -lv *.bcf )

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version 2>&1 | head -n1 | sed 's/^.*bcftools //; s/ .*\$//')
    END_VERSIONS
    """

    stub:
    def args = task.ext.args   ?: ''
    prefix   = task.ext.prefix ?: "${meta.id}"
    def index = args.contains("--write-index=tbi") || args.contains("-W=tbi") ? "tbi" :
                args.contains("--write-index=csi") || args.contains("-W=csi") ? "csi" :
                args.contains("--write-index") || args.contains("-W") ? "csi" :
                ""
    def create_index = index.matches("csi|tbi") ? "touch ${prefix}.vcf.gz.${index}" : ""
    """
    echo "" | gzip > ${prefix}.vcf.gz
    ${create_index}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version 2>&1 | head -n1 | sed 's/^.*bcftools //; s/ .*\$//')
    END_VERSIONS
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
    if (!file("${ch_fasta}.fai").exists()) {
        ch_fai = SAMTOOLS_FAIDX(ch_fasta)
    } else {
        ch_fai = Channel.fromPath("${ch_fasta}.fai")
    }
    ch_intervals = GENOME_INTERVALS(ch_fasta, ch_fai)
    | flatten
    | map {
        fname ->
        def meta = [:]
        meta.id = fname.simpleName.replace('intervals_', '') as int
        [meta, fname]
    }

    // Split each GVCF by intervals to reduce database loading times
    ch_bcf_single = ch_intervals
    | combine(
        ch_samplesheet
        | map { _meta, gvcf, tbi -> [gvcf, tbi] }
    ) // combine intervals with GVCFs
    | groupTuple(by: [0, 1])
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
