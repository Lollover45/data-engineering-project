/* This macro determines the unit of occurence measurement based on organism type according to the units used for their measurement in the datasets.

Each organism is measured using different units due to the nature of these organisms. 
The occurence of pests is measured by looking at how many bees were infected or how many of those pests bees carry.

For viruses, biologist do not count the presence of individual viruses, as that is not possible nor would it be relevant. 
It is more informative to look at how many bees are infected.

In the case of Nosema fungus, the number spores is high in infected bees, thus is reported as million spores per bee. 
Reporting the number of individual spores is not biologically relevant.

For Varroa, it is more useful looking at how many individuals were detected per 100 bees.
These are bee parasites and looking at their non-bee associated abundance is not relevant when conducting research into bee infections.

For bee occurences, counting the number of individual bees detected is the norm. 

*/

{% macro get_unit(col) %}
CASE
    WHEN {{ col }} = 'Apis mellifera' THEN 'individuals'
    WHEN {{ col }} = 'Varroa' THEN 'individuals per 100 bees'
    WHEN {{ col }} = 'Nosema' THEN 'million spores per bee'
    WHEN {{ col }} IN ('abpv','amsv1','cbpv','dwv','dwv-b','iapv','kbv','lsv2','sbpv','mkv')
        THEN '% of infected bees'
    ELSE 'Unknown'
END
{% endmacro %}
