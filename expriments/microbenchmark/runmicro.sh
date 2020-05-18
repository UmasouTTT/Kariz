#!/bin/bash


source ./config.sh


benchmark_dir=$PWD
echo "Start Benchmark from "${benchmark_dir}

declare -a strides=(0  4  8  16  24  32  48  64  80  96  112  128 144  160  176  192  208  224  240  256)



# Read the array values with space
for rep in $(seq 1 $reps); do 
   for stride in "${strides[@]}"; do
     echo -e "\e[96m Start ${app_name} experiment, stride ${stride}, repeat ${rep}\n\e[0m"
     ./warmup_cache.py ${stride}
     # warm up cache manually
     #cached_data=`ssh root@neu-3-41 /root/evict_from_cache.py ${stride}`
     #echo -e "\e[96m ${cached_data} \e\n[0m"

     cd ${hibench_root} 
     
     ${hibench_command} & 
     pid=$! 
  
     echo -e "\e[96m Wait for ${grace_time} to check app name\n\e[0m" 
     sleep ${grace_time} 
   
     app_id=`${benchmark_dir}/whichapp.py`
     echo -e "\e[96m Wait for ${app_id} ........\n\e[0m"
   
     wait ${pid}

     cd ${benchmark_dir}

     echo "${app_id},${app_name},${stride},${rep}" >> ${report_file}
     
   done

done


echo "Benchmark is done, gather results from ${report_file}"

