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


process makeIntervals {
    conda "bioconda::pysam:0.22.1"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/pysam:0.22.1--py39hcada746_0' :
        'quay.io/biocontainers/pysam:0.22.1--py39hcada746_0' }"

    input:
    path fasta_fn

    output:
    path "intervals_*.bed"

    script:
    """
    #!/usr/bin/env python
    import pysam
    import re

    fasta = pysam.FastaFile("${fasta_fn}")
    n = 1
    bedfile = open(f"intervals_{n}.bed", "w")
    tot = 0
    target = ${params.chunk_size}
    for seq_id in fasta.references:
        sequence = fasta[seq_id]
        for match in re.finditer('[ACGTacgt]+', str(sequence)):
            start = match.start()
            end = match.end()
            if tot > target:
                n += 1
                tot = 0
                bedfile.close()
                bedfile = open(f"intervals_{n}.bed", "w")
            tot += end - start
            bedfile.write(f'{seq_id}\\t{start}\\t{end}\\n')
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
    ch_itvs = makeIntervals(ch_fasta)
    ch_intervals = ch_itvs
    | flatten
    | map {
        fname ->
        def meta = [:]
        meta.id = fname.simpleName.replace('intervals_', '') as int
        [meta, fname]
    }

    // Collect the input VCFs to process
    // Run GLNEXUS on each dataset
    ch_input = ch_samplesheet
    | map {
        _meta, vcf -> 
        def new_meta = [:]
        new_meta.id = "joint_typing"
        [new_meta, vcf]
    }
    | groupTuple(by: 0)
    | collect
    ch_bcf_single = GLNEXUS(
        ch_input,
        ch_intervals
    )
    ch_tbi_single = BCFTOOLS_INDEX(ch_bcf_single.bcf)

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
