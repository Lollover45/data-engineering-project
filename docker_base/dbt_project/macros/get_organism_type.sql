/* This macro determines the organism type based on pre-established classification. 
The organisms and the types they belong to were pre-determined by previous steps in the project planning.

Nosema (mite) and Varroa (fungus) are categorized under "pest" together, because they are both living parasitic pest organisms. 

Viruses are categorized as "virus", because they are not considered to be living organisms.
*/

{% macro get_organism_type(col) %}
    CASE
        WHEN {{ col }} = 'Apis mellifera' THEN 'bee'
        WHEN {{ col }} IN ('Varroa', 'Nosema') THEN 'pest'
        WHEN {{ col }} IN ('abpv','amsv1','cbpv','dwv','dwv-b','iapv','kbv','lsv2','sbpv','mkv') THEN 'virus'
        ELSE 'Unknown'
    END
{% endmacro %}