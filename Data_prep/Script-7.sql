select t2.ay,t2.ortalama_sicaklik,t2.yil from "temp" t2 where t2.havza='F?rat-Dicle' 
and t2.model = 'HadGEM2-ES' and t2.senaryo = 'RCP4.5' and t2.grid = 1613 order by t2.ay,t2.yil;

 178.251.45.171