CFSAN Assembly Pipeline  
=======================  
  
Developed for CFSAN DAG pipeline manager ([http://10.12.207.89/justin.payne/dag-core](http://10.12.207.89/justin.payne/dag-core))  
    
    from dag_core import ClusterTask, FileTask, RegexTask, NullTask, OrmTask, RemapTask, CycleException
    
    
    
Functioning assembly pipeline and example workflow. At the top, we'll  
accept an ORM class of one of a list of appropriate types, collect SPAdes  
version information and trim if necessary, invoke SPAdes on the cluster,  
filter the short contigs from the assembly, count the contigs, parse the  
SPAdes output with a regex, then reformat the key-value data the workflow has  
accumulated and save a new SLims record. Lastly the workflow demonstrates  
that cyclic workflows are rejected by the workflow engine.  
    
It's a python script, so you can freely define in the local namespace:  
    filter_length = 500
    spades_module = ('spades/3.0.0/spades',)
    
Let's define some Tasks!  
    
OrmTask will dump a SLims record's fields into termargs. The termargs  
get passed along down the workflow graph.  
the first task you define is the workflow root by default.  
    root = OrmTask('* sequence')
    
cluster tasks run using UGE via SSH  
    version = ClusterTask("Get SPAdes version",
                         """spades.py --version""")
    
'input' and 'output' are convenience symbols with special meaning.  
  
'input' is bound to file paths from parent nodes that match any of the Unix shell  
patterns supplied to the optional argument 'file\_filter', or to all of them if  
no filters are supplied. The command will be submitted to the HPC in parallel, once  
per file that matches the filter patterns.  
  
'output' is bound to a scratch area, and files collected from it at the end are  
substituted for the input file if they're of the same extension. This lets you  
apply filters and other preprocessing to files:  
    trim = ClusterTask("FASTX Trim",
                     """fastx_trimmer -Q 33 -t 16 -i {input} -o {output}""",
                         file_filter = "*fastq*",
                         modules=('fastx'))
    
              
              
files in content fields are made available as the field name  
Files as non-field attachments aren't supplied to the workflow engine.  
    assemble_one = ClusterTask("SPAdes assembler (single-end)",
                             """spades.py -1 {cntn_fk_file} -t 8""",
                                modules=spades_module)
              
and as the names defined in gov.fda.cfsan.slims.util.Meta  
    assemble_pair = ClusterTask("SPAdes assembler (paired-end miseq)", 
                              """spades.py -1 {SEQ_MISEQ_FORWARD} 
                                           -2 {SEQ_MISEQ_REVERSE} 
                                           -o {output}
                                           -t 8""",
                                modules=spades_module)
                                           
    
the engine takes out tabs and linefeeds in commands, so commands can be written in  
an easy to read format.			     
    assemble_eight = ClusterTask("SPades assembler (paired-end nextseq)",
                               """spades.py -pe1-1 {SEQ_NEXTSEQ_FORWARD1}
                                            -pe1-2 {SEQ_NEXTSEQ_REVERSE1}
                                            -pe1-1 {SEQ_NEXTSEQ_FORWARD2}
                                            -pe1-2 {SEQ_NEXTSEQ_REVERSE2}
                                            -pe1-1 {SEQ_NEXTSEQ_FORWARD3}
                                            -pe1-2 {SEQ_NEXTSEQ_REVERSE3}
                                            -pe1-1 {SEQ_NEXTSEQ_FORWARD4}
                                            -pe1-2 {SEQ_NEXTSEQ_REVERSE4}
                                            -t 32
                                               -o {output}""",
                                modules=spades_module)
    
    
The constructor makes any extra keywords available to your command		     
    filter_contigs = ClusterTask("Filter sub-500 contigs",
                               """filter_contigs {length_filter}
                                  -i {input} 
                                  -o {output}""",
                                  length_filter = filter_length,
                                  file_filter="contigs.fasta")
                          
    count_contigs = ClusterTask("Assembly characterization",
                                "grep '^>' {input}")
        
    
    
now we wire up the graph by describing the relationships between tasks.  
    
you can explicitly set the root				  
    root.is_root()
    
wire up the tasks by describing which tasks follow others.  
    version.follows(root)
    
fluent API lets us describe conditional flows, too  
when, \_or, and \_and accept functions; the entire termargs structure is supplied  
so your functions should have the **kwargs catch-all. The truth value of your  
functions return type determines whether the workflow follows the conditional.  
Don't put expensive code here, or anywhere in a workflow description - these  
descriptions describe workflows and tasks and should evaluate very quickly.  
This workflow gets evaluated very frequently.  
  
note the leading underscores in \_or, \_and, and \_else, to get around Python's reserved  
keywords.  
    trim.follows(root) \
        .when(lambda cntn_fk_contentType, **k: \
            'Illumina' in cntn_fk_contentType) \
        ._else(assemble_one)
    
    assemble_pair.follows(trim) \
        .when(lambda cntn_fk_contentType, **k: \
            'Miseq' in cntn_fk_contentType) \
        ._else(assemble_eight)
    
    
These are directed acyclic graphs, not trees - a task can have multiple parents  
It won't start until all parents have completed (or been ignored due to conditionals.)  
    filter_contigs.follows(assemble_one) \
                  .follows(assemble_pair) \
                  .follows(assemble_eight)
    
Be aware - being ignored along one conditional branch will stop the task from starting  
even if it can be reached along another path in the graph.  
    
    count_contigs.follows(filter_contigs)
    
The regex task matches regex groups (see python's 're' module) in stdout from  
the previous tasks. It has an optional param ('stop\_on\_miss') to determine whether  
it stops the flow if there are no matches.  
    parse_output = RegexTask("Parse Assembler Output",
                             r"length_(?P<length>\d*)_cov_(?P<cov>\d*\.\d*)",
                             stop_on_miss=False) \
                            .follows(filter_contigs)
                     
Tasks provide decorators (Task.prep and Task.post) you can use to do mild pre or   
post-processing on data. This runs on the app server, though, so be extremely careful.  
They're called with  
    
 @parse.post  
 def avg\_coverage(  
 	for val in   
    
Remap tasks change keynames. This is a pretty common task for workflow-to-GIMS integration  
so you'll use it a lot at the end of workflows.  
if keyword "exclude" is set to true (default: false) then only the remapped key-values  
are passed on.  
    remap = RemapTask("Remap params to SLims fields",
                      cntn_cf_assemblyVersion="Version",
                      exclude=True) \
                     .follows(parse_output)
                      
                      
The NewORM task will save a new ORM record derived from the entry-point record (and  
linked to an instance of this workflow) but the content type pattern has to match a  
single content type, or an exception is thrown.  
  
  
    save = OrmTask('Assembly').follows(remap)
    
NullTasks do nothing but pass on termargs to subsequent tasks, but are useful for   
otherwise inconvenient graph structures (like when your workflow starts with two tasks)  
    
Cyclic structures can't be created; if you try, a CycleException is thrown in runtime:  
    try:
        null1 = NullTask()
        null2 = NullTask()
        null1.follows(null2)
        null2.follows(null1) #this throws CycleException
    except CycleException:
        del null2
        del null1
        
There's an easy test framework, too. Since workflow descriptions are loaded as modules  
in normal use, you can make testing the default run behavior under the standard Python  
guard:	  
    if __name__ == '__main__':
        root.test()
