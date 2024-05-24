-- Tomando como base data_model.agg_monthly, se une a cada cuenta el nombre (name_merchant) 
-- proveniente de reports.fz2_merchant_master adicional al uso de medios de cada medio de pago por mes

drop table if exists data_model_corrected;

select
	case
		when t2."name_merchant" is null then t1.account_name
		else t2."name_merchant"
	end as "name_merchant_",
	t1.billing_date_monthly,
	t2."group",
	min(t1.account_create_date) as account_create_date,
	max(case when t1.payment_method_type = 'PSE' then 1 else 0 end) as pse,
	max(case when t1.payment_method_type = 'CREDIT_CARD' then 1 else 0 end) as credit_card,
	max(case when t1.payment_method_type = 'LENDING' then 1 else 0 end) as lending,
	max(case when t1.payment_method_type = 'BANK_REFERENCED' then 1 else 0 end) as bank_referenced,
	max(case when t1.payment_method_type in ('CASH') then 1 else 0 end) as cash,
	max(case when t1.payment_method_type in ('CASH_ON_DELIVERY') then 1 else 0 end) as cash_on_delivery,
	max(case when t1.payment_method_type = 'REFERENCED' then 1 else 0 end) as referenced,
	max(case when t1.payment_method_type = 'BANK_TRANSFER' then 1 else 0 end) as bank_transfer,
	max(case when t1.payment_method_type = 'DEBIT_CARD' then 1 else 0 end) as debit_card,
	max(case when t1.payment_method_type = 'ACH' then 1 else 0 end) as ach
into
	data_model_corrected
from
	data_model.agg_data_model_monthly t1
left join reports.fz1_client_base t2
on
	t1.account_id = t2.account_id
	and last_day(t1.billing_date_monthly)= last_day(t2."month")
where
	/*"group" in ('SMB','Partnership') and*/
	country_account = 'CO'
group by
	"name_merchant_",
	t1.billing_date_monthly,
	t2."group";


-- Se agrupan los valores por (name_merchant) en términos económicos, provenientes de la tabla reports.fz1_client_base

drop table if exists fz1_grouped;

select
	name_merchant,
	"month",
	sum(gm_usd) as gm_usd,
	sum(tpt) as tpt,
	sum(tpv_usd) as tpv_usd,
	sum(revenue_usd) as revenue_usd
into
	fz1_grouped
from
	reports.fz1_client_base
where
	"group" in ('SMB', 'Partnership')
	and country_account = 'CO'
group by
	name_merchant,
	"month";


-- Se unen las tablas de billing_date por uso de medios de pago y las medidas económicas provenientes de fz1_client_base

drop table if exists data_model_corrected_2;
select t1.*,
t2.tpt,
t2.tpv_usd,
t2.revenue_usd,
t2.gm_usd
into data_model_corrected_2
from data_model_corrected t1
left join fz1_grouped t2
on t1.name_merchant_=t2.name_merchant and last_day(t1.billing_date_monthly)=last_day(t2."month");


-- Con una definición de Churn de 6 meses se evalúan mes a mes cuales son los comercios activos,
-- mediante un bucle que se ubica en cada mes y revisa los últimos 6 meses de procesamiento se determina 
-- la propiedad de actividad de los comercios.

create or replace procedure active_customers_2() language plpgsql as $$
declare cnt integer:= 1; x date:= '2020-01-01'; meses_churn integer := 6 ;
begin drop table if exists active_customers;
while cnt<37 loop
x = date(dateadd('month',-cnt,date_trunc('Month',date(getdate()))));

	if cnt = 1 then
	select 
	"name_merchant_" as name_merchant, 
	x as mes, 
	datediff('month',max(billing_date_monthly),x ) as inactividad, 
	"group"
	into active_customers
	from data_model_corrected_2
	where billing_date_monthly <= x and tpv_usd > 0 and tpt >0 and account_create_date <= last_day(x)
	group by "name_merchant_", x, "group"
	having inactividad < meses_churn;
	
	else
	insert into active_customers
	select 
	"name_merchant_" as name_merchant, 
	x as mes, datediff('month',max(billing_date_monthly),x ) as inactividad,
	"group"
	from data_model_corrected_2
	where billing_date_monthly <= x and tpv_usd > 0 and tpt >0 and account_create_date <= last_day(x)
	group by "name_merchant_", x, "group"
	having inactividad < meses_churn;
	end if;

/*raise info 'count: %',x;*/ cnt = cnt+1;
end loop; end; $$;

call active_customers_2();


-- Añado a la tabla de clientes activos su uso de cada medio de pago por mes.

drop table if exists active_customers_2;

select
	t1.name_merchant as nombre_merchant,
	t1.mes,
	t1.inactividad,
	t2.tpt,
	t2.tpv_usd,
	t2.revenue_usd,
	t2.gm_usd,
	t2.credit_card,
	t2.bank_referenced,
	t2.referenced,
	t2.bank_transfer,
	t2.pse,
	t2.debit_card,
	t2.cash,
	t2.cash_on_delivery,
	t2.ach,
	t2.lending
into
	active_customers_2
from
	active_customers t1
left join data_model_corrected_2 t2
on
	t1.name_merchant = t2.name_merchant_
	and t1.mes = date(t2.billing_date_monthly)
where
	t1."group" in ('SMB', 'Partnership') ;


-- Resumo la cantidad total de métodos utilizados, reviso cuales eran activos 
-- e identifico cuales eran inactivos cada mes (inactividad mayor a 3 meses)

drop table if exists economicas_nombre_metodos_act_inact;

select
	*,
	credit_card + bank_referenced + Referenced + bank_transfer + pse + debit_card + cash + cash_on_delivery + ach + lending as suma_de_metodos,
	case
		when inactividad >= 3 then 1
		else 0
	end as inactivo
into
	economicas_nombre_metodos_act_inact
from
	active_customers_2;


-- Se eliminan posibles duplicados

drop table if exists distinct_activos_nombre_pre;

select
	distinct *
into
	distinct_activos_nombre_pre
from
	economicas_nombre_metodos_act_inact;



-- Se reemplazan valores de null por 0 en métricas financieras

drop table if exists distinct_activos_nombre;

select
	nombre_merchant,
	mes,
	inactividad,
	case
		when tpt is null then 0
		else tpt
	end as tpt,
	case
		when tpv_usd is null then 0
		else tpv_usd
	end as tpv_usd,
	case
		when revenue_usd is null then 0
		else revenue_usd
	end as revenue_usd,
	case
		when gm_usd is null then 0
		else gm_usd
	end as gm_usd,
	credit_card,
	bank_referenced,
	referenced,
	bank_transfer,
	pse,
	debit_card,
	cash,
	cash_on_delivery,
	ach,
	lending,
	suma_de_metodos,
	inactivo
into
	distinct_activos_nombre
from
	distinct_activos_nombre_pre
	--where tpv_usd is null
;




-- Primera etapa del dashboard

drop table if exists dashboard_nombre;
select mes, count(1) as registros, sum(tpt) as tpt, sum(tpv_usd) tpv, sum(revenue_usd) as revenue, sum(gm_usd) as gm  
, sum(suma_de_metodos) as Suma_metodos, sum(inactivo) as inactivo 
into dashboard_nombre
from distinct_activos_nombre
group by mes 
/*order by mes*/
;



---------- cuento las creadas
-- creaciones
drop table if exists cuentas_creacion_nombre;
select name_merchant_ as nombre_merchant , date(date_trunc('month', (min(account_create_date)))) as fecha_creacion--, account_status 
into cuentas_creacion_nombre
from data_model_corrected_2
where /*account_create_date >= date(dateadd('month', -36, date_trunc('month',getdate()))) 
and*/ account_create_date < date(date_trunc('month',getdate())) 
group by nombre_merchant  ;







-- nombres creados
drop table if exists names_created;
select fecha_creacion , count(1) as cantidad
into names_created 
from (
select distinct date(date_trunc('month', t1.account_create_date )) as fecha_creacion 
, case when t2.name_merchant_2 is not null then  t2.name_merchant_2 else t3.nombre end as name_merchant  
from data_model.dim_account t1
left join client_base_nosotros t2 on t1.account_id = t2.account_id 
left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id 
where t1.account_id in (select cast(account_id as varchar) from cruce_smb) 
and t1.account_id not in (1, 2, 5, 501159, 515992,592315)) tr
group by fecha_creacion
order by fecha_creacion desc; 




-- cuentas creadas
drop table if exists account_created;
select fecha_creacion, count(1) as cantidad
into account_created
from (
select account_id, date(date_trunc('month', account_create_date )) as fecha_creacion 
from data_model.dim_account 
where account_id in (select cast(account_id as varchar) from cruce_smb)
and account_id not in (1, 2, 5, 501159, 515992,592315)) ta
group by fecha_creacion 
order by fecha_creacion desc;

select *
from account_created ac 
order by fecha_creacion desc
limit 10;

--cuentos mes de primera trx
-- primera trx
drop table if exists primera_transaccion_nombre;
select name_merchant_ as nombre_merchant  , min(billing_date_monthly) as primera_trx
into primera_transaccion_nombre 
from data_model_corrected_2 
where billing_date_monthly =< date(date_trunc('month',getdate())) and tpv_usd > 0 and tpt >0
group by nombre_merchant ;


-- conteo primera transacción por mes
drop table if exists primera_transaccion_nombre_es;
select primera_trx, count(1) as cantidad_primera_trx
into primera_transaccion_nombre_es
from primera_transaccion_nombre
group by primera_trx
/*order by primera_trx*/ ;



-- uno las creadas y la primera trx a la tabla que venía con los indicadores, mobs por creación y 
drop table if exists activos_nombres_mobs;
select t1.*
, t2.fecha_creacion as fecha_creacion_nombre
, t3.primera_trx as primera_trx_nombre
, datediff('month', fecha_creacion_nombre, primera_trx_nombre) as creacion_hasta_first_trx
, datediff('month', primera_trx_nombre, t1.mes) as mob_first_trx 
, datediff('month', fecha_creacion_nombre, t1.mes) as mob_creacion_cuenta
, case when creacion_hasta_first_trx = 0 and mes = primera_trx_nombre then 'activa_mes_creada' else null end as created_activated
into activos_nombres_mobs
from distinct_activos_nombre t1 
left join cuentas_creacion_nombre t2 on t1.nombre_merchant = t2.nombre_merchant 
left join primera_transaccion_nombre t3 on t1.nombre_merchant = t3.nombre_merchant
/*order by nombre_merchant, mes
limit 50*/; 






-- Añado el mob de primera trx con la diferencia entre la fecha de creación y la primera trx
-- miro cuantas se activaron por mes (sin mirar estado de cuenta)
-- también sacó el porcentaje acumulado para hacer las curvas
drop table if exists first_trx_curve_nombre;
select tx.fecha_creacion, tx.mob_first_trx, count(1) as activas_mes_mob
, t3.cantidad as total_creadas
, case when lag(tx.fecha_creacion, 1) over(order by tx.fecha_creacion, tx.mob_first_trx)  = tx.fecha_creacion 
	then	
	sum(activas_mes_mob) over (partition by tx.fecha_creacion order by tx.fecha_creacion, tx.mob_first_trx  	rows unbounded preceding) 
	else activas_mes_mob 
end as cumulative_sum
, cast( cumulative_sum as decimal(10,5)) / cast(total_creadas as decimal(10,5)) as tasa_activacion_acum
,  extract(month from tx.fecha_creacion)+extract(year from tx.fecha_creacion)* 100 as fm_num
into  first_trx_curve_nombre
from (
select t1.*,
t2.primera_trx
, datediff('month', t1.fecha_creacion, t2.primera_trx) as mob_first_trx
from cuentas_creacion_nombre t1 
left join primera_transaccion_nombre t2 on t1.nombre_merchant  = t2.nombre_merchant) tx
left join names_created t3 on t3.fecha_creacion = tx.fecha_creacion 
where mob_first_trx is not null
group by tx.fecha_creacion, tx.mob_first_trx, t3.cantidad  
order by tx.fecha_creacion desc, tx.mob_first_trx  ;


-- se imprime la tabla anterior y se pega al dashboard (curvas de tasa de activación)  pestaña First trx curve
select *
from first_trx_curve_nombre
where fecha_creacion >= '2019-04-01'
order by fecha_creacion desc
--limit 1
;




-- resumen economico con fecha de creación (uno de los que se imprimía)
select fecha_creacion_nombre ,  mob_creacion_cuenta
, avg(tpt) as tpt, avg(tpv_usd) as tpv, avg(revenue_usd) as revenue, count(1) as cantidad
from activos_nombres_mobs 
group by fecha_creacion_nombre, mob_creacion_cuenta 
order by fecha_creacion_nombre desc, mob_creacion_cuenta asc;


--resumen economico con fecha de primera trx -- se imprime, pestaña 'Mobs first trx (Avg)'
select primera_trx_nombre  ,  mob_first_trx ,  avg(tpt) as tpt, avg(tpv_usd) as tpv, avg(revenue_usd) as revenue, count(1) as cantidad
from activos_nombres_mobs 
group by primera_trx_nombre, mob_first_trx 
order by primera_trx_nombre desc, mob_first_trx asc;




-- back from churn -- calculo cuales volvieron en qué mes

drop table if exists vuelven_churn_mes;
select mes, back_from_Churn, count(1) as number_back 
into vuelven_churn_mes
from(
select nombre_merchant , mes, inactividad
, case when lag(nombre_merchant , 1) over(order by nombre_merchant , mes) =  nombre_merchant 
then lag(mes , 1) over(order by  nombre_merchant , mes) else null end as mes_anterior
, case when datediff('month', mes, Mes_anterior) <> -1 then 'is back'
else null end as back_from_Churn
from activos_nombres_mobs
) t1
where back_from_Churn = 'is back'
group by mes, back_from_Churn
order by mes, back_from_Churn;

-- revisión back from churn
select nombre_merchant 
from
(
select nombre_merchant , mes, inactividad
, case when lag(nombre_merchant , 1) over(order by nombre_merchant , mes) =  nombre_merchant 
then lag(mes , 1) over(order by  nombre_merchant , mes) else null end as mes_anterior
, case when datediff('month', mes, Mes_anterior) <> -1 then 'is back'
else null end as back_from_Churn
from activos_nombres_mobs ) t1
where mes = '2022-03-01';


-- tabla en la que cuento cuantos hicieron primera trx el mismo mes de creación 
drop table if exists primera_trx_mes_creacion_table;
select mes, count(1) as primera_trx_mes_creacion
into primera_trx_mes_creacion_table
from  activos_nombres_mobs
where created_activated is not null
group by mes
order by mes;



--- -- saldo promedio
drop table if exists saldo_promedio;
select cuenta_id as account_id
, date(date_trunc('month', fecha_creacion)) as fecha_saldo
,  avg(saldo) as saldo 
into saldo_promedio
from staging.polv4_pps_imp_corte_cuenta
Where cuenta_id in (select account_id from cruce_smb)
and account_id not in (1, 2, 5, 501159, 515992,592315)
group by cuenta_id, fecha_saldo;




-- ultimo saldo del mes
drop table if exists saldo_fin_mes;
select cuenta_id as account_id , date(date_trunc('month', fecha_creacion)) as fecha_saldo, saldo
, case when date(fecha_creacion) = last_day(fecha_creacion) then 'last' else null end as es_ultimo
into saldo_fin_mes
from staging.polv4_pps_imp_corte_cuenta ppicc 
where es_ultimo is not null and cuenta_id in (select account_id from cruce_smb)
and cuenta_id  not in (1, 2, 5, 501159, 515992,592315)
order by account_id, fecha_creacion;

-- agrupación con saldos al estilo de la tabla de activos
drop table if exists saldos_nombres;
select name_merchant, fecha_saldo, sum(saldo_promedio_mes) as saldo_promedio_mes, sum(saldo_f_mes) as saldo_f_mes
into saldos_nombres
from (
select 
distinct case when t3.name_merchant_2 is not null then  t3.name_merchant_2 else t4.nombre end as name_merchant
, t1.fecha_saldo
, cast(t1.saldo as decimal(20,3))/cast(t5.usd as decimal(20,3)) as saldo_promedio_mes
, cast(t2.saldo as decimal(20,3))/cast(t5.usd as decimal(20,3)) as saldo_f_mes
--into saldos_nombres 
from saldo_promedio t1
left join saldo_fin_mes t2 on t1.account_id = t2.account_id and t1.fecha_saldo = t2.fecha_saldo
left join client_base_nosotros t3 on t1.account_id = t3.account_id
left join staging.polv4_pps_imp_cuenta t4 on t1.account_id = t4.cuenta_id 
left join reports.trm_current t5 
on extract(year from t1.fecha_saldo) = t5."year" and extract(month from t1.fecha_saldo) = t5."month" 
and t5.currency = 'COP'
where t1.account_id in (select cast(account_id as varchar) from cruce_smb)
and t1.account_id not in (1, 2, 5, 501159, 515992,592315) ) tabla
group by name_merchant, fecha_saldo
--group by name_merchant
;




---------




-- unir saldos a la tabla de activos y de paso lo de back from churn
drop table if exists activos_nombre_saldos;
select back.*, saldo_promedio_mes, saldo_f_mes
into activos_nombre_saldos
from (
select *
, case when lag(nombre_merchant , 1) over(order by nombre_merchant , mes) =  nombre_merchant 
then lag(mes , 1) over(order by  nombre_merchant , mes) else null end as mes_anterior
, case when datediff('month', mes, Mes_anterior) <> -1 then 'is back'
else null end as back_from_Churn
from activos_nombres_mobs ) back
left join saldos_nombres t2 on back.nombre_merchant = t2.name_merchant and back.mes = t2.fecha_saldo 
;
--------





-- saldos agrupados para dashboard
drop table if exists saldos_nombre;
select mes
, avg(saldo_promedio_mes) as saldo_promedio_mes_Nombre, avg(saldo_f_mes) as saldo_f_mes_nombre
, sum(saldo_promedio_mes) as saldo_promedio_mes_sum, sum(saldo_f_mes) as saldo_f_mes_sum 
into saldos_nombre
from activos_nombre_saldos
group by mes
order by mes;






-- INICIO CHURN

create or replace procedure active_customers_2_churn() language plpgsql as $$
declare cnt integer:= 1; x date:= '2020-01-01'; meses_churn integer := 6 ;
begin drop table if exists active_customers_churn;
while cnt<37 loop
x = date(dateadd('month',-cnt,date_trunc('Month',date(getdate()))));

	if cnt = 1 then
	select "name_merchant_" as nombre_merchant, x as mes, datediff('month',max(billing_date_monthly),x ) as inactividad
	, "group"
	into active_customers_churn
	from data_model_corrected_2
	where billing_date_monthly <= x and tpv_usd > 0 and tpt >0 and account_create_date <= last_day(x)
	group by "name_merchant_", x, "group"
	having inactividad = meses_churn;
	
	else
	insert into active_customers_churn
	select "name_merchant_" as nombre_merchant, x as mes, datediff('month',max(billing_date_monthly),x ) as inactividad
	, "group"
	from data_model_corrected_2
	where billing_date_monthly <= x and tpv_usd > 0 and tpt >0 and account_create_date <= last_day(x)
	group by "name_merchant_", x, "group"
	having inactividad = meses_churn;
	end if;

/*raise info 'count: %',x;*/ cnt = cnt+1;
end loop; end; $$;

call active_customers_2_churn();

-- resumo el churn por mes
drop table if exists resumen_churn_nombre;
select mes, count(1) as cantidad_churn 
into resumen_churn_nombre
from active_customers_churn
where "group" in ('SMB','Partnership') 
group by mes
order by mes ;





-- Sección calculo fraude
-- transacciones con fraude
drop table if exists transacciones_fraude_nombre;
select t1.usuario_id, t1.transaccion_id, t1.orden_id
into transacciones_fraude_nombre
from staging.polv4_pps_imp_transaccion t1
inner join staging.polv4_pps_imp_disputa t2 on t1.orden_id = t2.orden_id 
and t2.motivo = 'FRAUD' and t2.estado = 'REFUNDED';





-- Traigo la cuenta de la tabla orden para fraude
drop table if exists transacciones_fraude_cuenta_pre_nombre ;
select t2.cuenta_id, t1.*
into transacciones_fraude_cuenta_pre_nombre
from transacciones_fraude_nombre  t1 
inner join staging.polv4_pps_imp_orden t2 on t1.orden_id = t2.orden_id
where t2.cuenta_id in (select cast(account_id as varchar) from cruce_smb)
and t2.cuenta_id not in (1, 2, 5, 501159, 515992,592315) ;


-- traigo monto y nombre de fraude
drop table if exists fraude_agrupado;
select distinct 
case when t3.name_merchant_2 is not null then  t3.name_merchant_2 else t4.nombre end as name_merchant
, t1.*
, date(date_trunc('month', t5.fecha_creacion)) as fecha_creacion_nombre
, cast(t5.pm_purchase_value as decimal(20,3))/cast(t6.usd as decimal(20,3)) as pm_purchase_value
into fraude_agrupado
from transacciones_fraude_cuenta_pre_nombre t1
left join client_base_nosotros t3 on t1.cuenta_id = t3.account_id
left join staging.polv4_pps_imp_cuenta t4 on t1.cuenta_id = t4.cuenta_id 
left join staging.polv4_pps_imp_transaccion_montos_adicionales t5 on t1.transaccion_id = t5.transaccion_id
left join reports.trm_current t6
on extract(year from date(date_trunc('month', t5.fecha_creacion))) = t6."year" 
and extract(month from date(date_trunc('month', t5.fecha_creacion))) = t6."month" 
and t6.currency = 'COP'
where t1.cuenta_id in (select cast(account_id as varchar) from cruce_smb)
and t1.cuenta_id not in (1, 2, 5, 501159, 515992,592315)
;




--agrupo por nombre y fecha fraude
drop table if exists fraude_nombre_fecha;
select name_merchant, fecha_creacion_nombre, sum(pm_purchase_value) as monto_fraude, count(1) as cantidad_fraude
into fraude_nombre_fecha
from fraude_agrupado
group by name_merchant, fecha_creacion_nombre;

-- cuento cuantos hubo en total de fraude y su monto por fecha
drop table if exists resumen_fraude_nombre;
select fecha_creacion_nombre, SUM(cantidad_fraude) as cantidad_fraud, sum(monto_fraude) as monto_fraude
into resumen_fraude_nombre
from fraude_nombre_fecha
group by fecha_creacion_nombre
;





--- a activos le pego el fraude 
drop table if exists activos_nombre_fraude;
select t1.*
, case when t2.monto_fraude is null then 0 else t2.monto_fraude end as monto_fraude
, case when t2.cantidad_fraude is null then 0 else t2.cantidad_fraude end as cantidad_fraude
into activos_nombre_fraude
from activos_nombre_saldos  t1
left join fraude_nombre_fecha t2 on t1.nombre_merchant = t2.name_merchant 
and t1.mes = t2.fecha_creacion_nombre;



-- uno al dashboard las cosas que voy avanzando
-- creadas, vuelven de churn, priemra trx, primera trx mes creacion, saldo, fraude
--drop table if exists dashboard_mar25;
select t1.*, t2.cantidad as cantidad_nom_creados,  t4.number_back 
, t5.cantidad_primera_trx
, (cantidad_nom_creados - t6.primera_trx_mes_creacion) as cantidad_creados_no_trx
, t6.primera_trx_mes_creacion
, t7.saldo_promedio_mes_Nombre
, t7.saldo_f_mes_nombre
, t7.saldo_promedio_mes_sum
, t7.saldo_f_mes_sum 
, t8.cantidad_churn
, t9.monto_fraude
, t9.cantidad_fraud
--into dashboard_mar25
from dashboard_nombre t1
left join names_created t2 on t1.mes = t2.fecha_creacion  
left join vuelven_churn_mes t4 on t1.mes = t4.mes 
left join primera_transaccion_nombre_es t5 on t1.mes = t5.primera_trx
left join primera_trx_mes_creacion_table t6 on t1.mes = t6.mes
left join saldos_nombre t7 on t1.mes = t7.mes
left join resumen_churn_nombre t8 on t1.mes = t8.mes
left join resumen_fraude_nombre t9 on t1.mes = t9.fecha_creacion_nombre
order by t1.mes desc
limit 37;



 
 
-- se pega el mcc tomando a consideración el mcc de la cuenta que tiene mayor tpt
drop table if exists activos_nombre_mcc;
select distinct t8.*, t7.codigo_mcc_id
into activos_nombre_mcc
from activos_nombre_fraude t8
left join (
	select t5.nombre_merchant, t5.account_id, t6.codigo_mcc_id
	from (
		select nombre_merchant, account_id, tpt_total
		, case when lag(nombre_merchant, 1) over(order by nombre_merchant, tpt_total desc) = nombre_merchant then
		0 else 1 end as lag_max
		from (
			select distinct
			case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t3.nombre end as nombre_merchant
			, t1.account_id, t1.tpt_total 
			from 
				(select account_id, /*date(date_trunc('month',"month")) as mes ,*/ sum(tpt) as tpt_total
				from reports.fz1_client_base  
				where account_id in (select cast(account_id as varchar) as account_id  from cruce_smb) 
				and account_id not in (1, 2, 5, 501159, 515992,592315)
				group by account_id) t1
			left join client_base_nosotros t2 on t1.account_id  = t2.account_id
			left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id ) t4
			) t5
	left join staging.polv4_pps_imp_cuenta t6 on t5.account_id = t6.cuenta_id and cuenta_id 
	in  (select cast(account_id as varchar) as account_id  from cruce_smb) 
	where lag_max =1) 
	t7 on t8.nombre_merchant = t7.nombre_merchant 
order by nombre_merchant, mes 	
;





---------------------------------------------------------------------------
-- modelo pagos, origen, referido, ciudad
--- 
drop table if exists activos_nombre_c_m_f_r;--drop table if exists activos_nombre_mcc;
select distinct t8.*, t7.modelo_pagos, t7.fuente_creacion
,  case when  t7.referido_cuenta_linea is null then 'No' else 'Yes' end as referido_cuenta_linea
, t7.ciudad
into activos_nombre_c_m_f_r--into activos_nombre_mcc
from activos_nombre_mcc t8
left join (
	select t5.nombre_merchant, t5.account_id, t6.modelo_pagos, t6.fuente_creacion, t6.referido_cuenta_linea, t6.ciudad
	from (
		select nombre_merchant, account_id, tpt_total
		, case when lag(nombre_merchant, 1) over(order by nombre_merchant, tpt_total desc) = nombre_merchant then
		0 else 1 end as lag_max
		from (
			select distinct
			case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t3.nombre end as nombre_merchant
			, t1.account_id, t1.tpt_total 
			from 
				(select account_id, /*date(date_trunc('month',"month")) as mes ,*/ sum(tpt) as tpt_total
				from reports.fz1_client_base  
				where account_id in (select cast(account_id as varchar) as account_id  from cruce_smb )
				and account_id not in (1, 2, 5, 501159, 515992,592315)
				group by account_id) t1
			left join client_base_nosotros t2 on t1.account_id  = t2.account_id
			left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id ) t4
			) t5
	left join staging.polv4_pps_imp_cuenta t6 on t5.account_id = t6.cuenta_id and cuenta_id 
	in  (select cast(account_id as varchar) as account_id  from cruce_smb) 
	where lag_max =1) 
	t7 on t8.nombre_merchant = t7.nombre_merchant 
order by nombre_merchant, mes 	
;




-----------------------

-- estandarizando ciudad
drop table if exists activos_nombre_ciudad;
select *, case when ciudad ilike '%ogot%' then 'Bogotá and surroundings'
when ciudad ilike '%Medell%' then 'Medellín and surroundings'
when ciudad ilike '%cali' then 'Cali'
when ciudad ilike '%cartagen%' then 'Cartagena'
when ciudad ilike '%envigado%' then 'Medellín and surroundings'
when ciudad ilike '%itag%' then 'Medellín and surroundings'
when ciudad ilike '%barranq%' then 'Barranquilla'
when ciudad ilike '%bucarama%' then 'Bucaramanga'
when ciudad ilike '%chía%' then 'Bogotá and surroundings'
when ciudad ilike '%chia%' and ciudad <> 'QUINCHIA' then 'Bogotá and surroundings'
else 'Others' end as City
into activos_nombre_ciudad
from activos_nombre_c_m_f_r
;


-- tarifas por cuenta de los medios de pago que consideramos
drop table if exists tarifas_x_cuenta;
select  t3.cuenta_id, t1.comision_plana_comercio, t1.comision_porcentual_comercio, t2.tipo_medio_pago 
into tarifas_x_cuenta
from staging.polv4_pps_imp_comision_pricing t1
left join staging.polv4_pps_imp_perfil_cobranza t2 on t1.perfil_cobranza_id = t2.perfil_cobranza_id
left join staging.polv4_pps_imp_cuenta t3 on t2.grupo_perfil_cobranza_id = t3.grupo_perfil_cobranza_id
where t2.tipo_medio_pago in ('VISA','MASTERCARD', 'PSE', 'EFECTY')
;



-- añadiendo medio de pago
drop table if exists activos_nombre_comision;--drop table if exists activos_nombre_mcc;
select distinct t8.*
	, t7.Plana_Visa, t7.Porcentual_Visa
	, t7.Plana_MC, t7.Porcentual_MC
	, t7.Plana_PSE, t7.Porcentual_PSE
	, t7.Plana_Efecty, t7.Porcentual_Efecty
into activos_nombre_comision--into activos_nombre_mcc
from activos_nombre_ciudad t8
left join (
	select t5.nombre_merchant, t5.account_id
	, t111.comision_plana_comercio as Plana_Visa, t111.comision_porcentual_comercio as Porcentual_Visa
	, t222.comision_plana_comercio as Plana_MC, t222.comision_porcentual_comercio as Porcentual_MC
	, t333.comision_plana_comercio as Plana_PSE, t333.comision_porcentual_comercio as Porcentual_PSE
	, t444.comision_plana_comercio as Plana_Efecty, t444.comision_porcentual_comercio as Porcentual_Efecty
	from (
		select nombre_merchant, account_id, tpt_total
		, case when lag(nombre_merchant, 1) over(order by nombre_merchant, tpt_total desc) = nombre_merchant then
		0 else 1 end as lag_max
		from (
			select distinct
			case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t3.nombre end as nombre_merchant
			, t1.account_id, t1.tpt_total 
			from 
				(select account_id, /*date(date_trunc('month',"month")) as mes ,*/ sum(tpt) as tpt_total
				from reports.fz1_client_base  
				where account_id in (select cast(account_id as varchar) as account_id  from cruce_smb) 
				and account_id not in (1, 2, 5, 501159, 515992,592315)
				group by account_id) t1
			left join client_base_nosotros t2 on t1.account_id  = t2.account_id
			left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id ) t4
			) t5
	left join staging.polv4_pps_imp_cuenta t6 on t5.account_id = t6.cuenta_id and cuenta_id 
	in  (select cast(account_id as varchar) as account_id  from cruce_smb)
	left join tarifas_x_cuenta t111 on t111.cuenta_id = t6.cuenta_id 
		and t111.tipo_medio_pago = 'VISA' 
	left join tarifas_x_cuenta t222 on t222.cuenta_id = t6.cuenta_id 
		and t222.tipo_medio_pago = 'MASTERCARD' 
	left join tarifas_x_cuenta t333 on t333.cuenta_id = t6.cuenta_id 
		and t333.tipo_medio_pago = 'PSE' 
	left join tarifas_x_cuenta t444 on t444.cuenta_id = t6.cuenta_id 
		and t444.tipo_medio_pago = 'EFECTY'
	where lag_max =1) 
	t7 on t8.nombre_merchant = t7.nombre_merchant and t8.mes = '2022-04-01'
order by nombre_merchant, mes 	
;


----------------

drop table if exists activos_nombre_rfm;
select t1.*
, t2.recencia, t2.alt_baj_recencia
, t2.frecuencia_internet, t2.alt_baj_frecuencia
, t2.monto, t2.alt_baj_monto
, t2.rfm_group
into activos_nombre_rfm
from activos_nombre_comision t1
left join RFM_cruzado_gerenciados t2 on t1.nombre_merchant = t2.nombre 
and t1.mes = date(date_trunc('month', t2.fecha_mes))
--limit 200
;



drop table if exists alterno_activos_nombre_rfm;
select distinct  nombre_merchant, mes, inactividad, tpt, tpv_usd, revenue_usd, gm_usd, credit_card, bank_referenced, referenced, bank_transfer, pse, debit_card, cash, cash_on_delivery, ach, lending, suma_de_metodos, inactivo, fecha_creacion_nombre, primera_trx_nombre, creacion_hasta_first_trx, mob_first_trx, mob_creacion_cuenta, created_activated, mes_anterior, back_from_churn, saldo_promedio_mes, saldo_f_mes, monto_fraude, cantidad_fraude, codigo_mcc_id, modelo_pagos, fuente_creacion, referido_cuenta_linea, ciudad, city, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
into alterno_activos_nombre_rfm
from activos_nombre_rfm;

-- cuento los que vuelven de churn
drop table if exists agrupado_back_from_churn;
select mes, back_from_churn, count(1) as cantidad_back
, codigo_mcc_id, modelo_pagos, fuente_creacion
, case when  referido_cuenta_linea is null then 'No' else 'Yes' end as Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
, rfm_group
into agrupado_back_from_churn
from activos_nombre_rfm 
where back_from_churn is not null
group by back_from_churn, mes, codigo_mcc_id, modelo_pagos, fuente_creacion
,  Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
, rfm_group
;


--- primera trx el mes de creación
drop table if exists agrupado_created_activated;
select mes, created_activated, count(1) as trx_creation_month
, codigo_mcc_id, modelo_pagos, fuente_creacion
, case when  referido_cuenta_linea is null then 'No' else 'Yes' end as referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
, rfm_group
into agrupado_created_activated
from activos_nombre_rfm 
where created_activated is not null
group by created_activated, mes, codigo_mcc_id, modelo_pagos, fuente_creacion
,  Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
, rfm_group
;

--- primera trx sin importar cuando se creó
drop table if exists agrupado_primera_trx;
select  primera_trx_nombre, count(1) as cantidad_sin_imp_1_trx
, codigo_mcc_id, modelo_pagos, fuente_creacion
, case when  referido_cuenta_linea is null then 'No' else 'Yes' end as Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
, rfm_group
into agrupado_primera_trx
from activos_nombre_rfm 
group by primera_trx_nombre, codigo_mcc_id, modelo_pagos, fuente_creacion
,  Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
, rfm_group
;




drop table if exists Activos_flag ;
select *, 'Activos' as Estado_ 
into Activos_flag
from alterno_activos_nombre_rfm
--limit 5
;



--- traigo t7.codigo_mcc_id, t7.modelo_pagos, t7.fuente_creacion, t7.referido_cuenta_linea, t7.ciudad
drop table if exists churn_nombre_variables_cuenta;--creados_nombre_cuenta_;--drop table if exists activos_nombre_mcc;
select distinct t8.*, t7.codigo_mcc_id, t7.modelo_pagos, t7.fuente_creacion, t7.referido_cuenta_linea, t7.ciudad
into churn_nombre_variables_cuenta--creados_nombre_cuenta_--into activos_nombre_mcc
from active_customers_churn t8 --active_customers_churn t8
left join (
	select t5.nombre_merchant, t5.account_id, t6.codigo_mcc_id, t6.modelo_pagos, t6.fuente_creacion, t6.referido_cuenta_linea, t6.ciudad
	from (
		select nombre_merchant, account_id, tpt_total
		, case when lag(nombre_merchant, 1) over(order by nombre_merchant, tpt_total desc) = nombre_merchant then
		0 else 1 end as lag_max
		from (
			select distinct
			case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t3.nombre end as nombre_merchant
			, t1.account_id, t1.tpt_total 
			from 
				(select account_id, /*date(date_trunc('month',"month")) as mes ,*/ sum(tpt) as tpt_total
				from reports.fz1_client_base  
				where account_id in (select cast(account_id as varchar) as account_id  from cruce_smb) 
				and account_id not in (1, 2, 5, 501159, 515992,592315)
				group by account_id) t1
			left join client_base_nosotros t2 on t1.account_id  = t2.account_id
			left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id ) t4
			) t5
	left join staging.polv4_pps_imp_cuenta t6 on t5.account_id = t6.cuenta_id and cuenta_id 
	in  (select cast(account_id as varchar) as account_id  from cruce_smb) 
	where lag_max =1) 
	t7 on t8.nombre_merchant = t7.nombre_merchant 
order by nombre_merchant, mes 	
;

drop table if exists churn_nombre_ciudad;
select *, case when ciudad ilike '%ogot%' then 'Bogotá and surroundings'
when ciudad ilike '%Medell%' then 'Medellín and surroundings'
when ciudad ilike '%cali' then 'Cali'
when ciudad ilike '%cartagen%' then 'Cartagena'
when ciudad ilike '%envigado%' then 'Medellín and surroundings'
when ciudad ilike '%itag%' then 'Medellín and surroundings'
when ciudad ilike '%barranq%' then 'Barranquilla'
when ciudad ilike '%bucarama%' then 'Bucaramanga'
when ciudad ilike '%chía%' then 'Bogotá and surroundings'
when ciudad ilike '%chia%' and ciudad <> 'QUINCHIA' then 'Bogotá and surroundings'
else 'Others' end as City
into churn_nombre_ciudad
from churn_nombre_variables_cuenta
;


-- añadiendo medio de pago
drop table if exists churn_nombre_comision;--drop table if exists activos_nombre_mcc;
select distinct t8.*
	, t7.Plana_Visa, t7.Porcentual_Visa
	, t7.Plana_MC, t7.Porcentual_MC
	, t7.Plana_PSE, t7.Porcentual_PSE
	, t7.Plana_Efecty, t7.Porcentual_Efecty
into churn_nombre_comision--into activos_nombre_mcc
from churn_nombre_ciudad t8
left join (
	select t5.nombre_merchant, t5.account_id
	, t111.comision_plana_comercio as Plana_Visa, t111.comision_porcentual_comercio as Porcentual_Visa
	, t222.comision_plana_comercio as Plana_MC, t222.comision_porcentual_comercio as Porcentual_MC
	, t333.comision_plana_comercio as Plana_PSE, t333.comision_porcentual_comercio as Porcentual_PSE
	, t444.comision_plana_comercio as Plana_Efecty, t444.comision_porcentual_comercio as Porcentual_Efecty
	from (
		select nombre_merchant, account_id, tpt_total
		, case when lag(nombre_merchant, 1) over(order by nombre_merchant, tpt_total desc) = nombre_merchant then
		0 else 1 end as lag_max
		from (
			select distinct
			case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t3.nombre end as nombre_merchant
			, t1.account_id, t1.tpt_total 
			from 
				(select account_id, /*date(date_trunc('month',"month")) as mes ,*/ sum(tpt) as tpt_total
				from reports.fz1_client_base  
				where account_id in (select cast(account_id as varchar) as account_id  from cruce_smb) 
				and account_id not in (1, 2, 5, 501159, 515992,592315)
				group by account_id) t1
			left join client_base_nosotros t2 on t1.account_id  = t2.account_id
			left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id ) t4
			) t5
	left join staging.polv4_pps_imp_cuenta t6 on t5.account_id = t6.cuenta_id and cuenta_id 
	in  (select cast(account_id as varchar) as account_id  from cruce_smb)
	left join tarifas_x_cuenta t111 on t111.cuenta_id = t6.cuenta_id 
		and t111.tipo_medio_pago = 'VISA' 
	left join tarifas_x_cuenta t222 on t222.cuenta_id = t6.cuenta_id 
		and t222.tipo_medio_pago = 'MASTERCARD' 
	left join tarifas_x_cuenta t333 on t333.cuenta_id = t6.cuenta_id 
		and t333.tipo_medio_pago = 'PSE' 
	left join tarifas_x_cuenta t444 on t444.cuenta_id = t6.cuenta_id 
		and t444.tipo_medio_pago = 'EFECTY'
	where lag_max =1) 
	t7 on t8.nombre_merchant = t7.nombre_merchant and t8.mes = '2022-04-01'
order by nombre_merchant, mes 	
;


-- añadiendo rfm
drop table if exists churn_nombre_rfm;
select t1.*
, t2.recencia, t2.alt_baj_recencia
, t2.frecuencia_internet, t2.alt_baj_frecuencia
, t2.monto, t2.alt_baj_monto
, t2.rfm_group
into churn_nombre_rfm
from churn_nombre_comision t1
left join RFM_cruzado_gerenciados t2 on t1.nombre_merchant = t2.nombre 
and t1.mes = date(date_trunc('month', t2.fecha_mes))
--limit 200
;


-- agrupación por mes para pegar
drop table if exists agrupacion_churn_nombre_variables;
select mes, count(1) as cantidad_churn
, codigo_mcc_id, modelo_pagos, fuente_creacion
,  case when  referido_cuenta_linea is null then 'No' else 'Yes' end as Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
--, rfm_group
into agrupacion_churn_nombre_variables
from churn_nombre_rfm
group by mes, codigo_mcc_id, modelo_pagos, fuente_creacion
,  Referidot,  city
, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
order by mes
;

-- corrigiendo Churn
drop table if exists Churn_flag ;
select *, 'Churn' as Estado_ 
into Churn_flag
from churn_nombre_rfm
--limit 5
;
drop table if exists churn_flag_corregido;
select nombre_merchant,	 mes,	 inactividad, 
null as  tpt,	null as  tpv_usd,	null as  revenue_usd,	null as  gm_usd,	null as  credit_card,	null as  bank_referenced,	null as  referenced,	null as  bank_transfer,	null as  pse,	null as  debit_card,	null as  cash,	null as  cash_on_delivery,	null as  ach,	null as  lending,	null as  suma_de_metodos,	null as  inactivo,	null as  fecha_creacion_nombre,	null as  primera_trx_nombre,	null as  creacion_hasta_first_trx,	null as  mob_first_trx,	null as  mob_creacion_cuenta,	null as  created_activated,	null as  mes_anterior,	null as  back_from_churn,	null as  saldo_promedio_mes,	null as  saldo_f_mes,	null as  monto_fraude,	null as  cantidad_fraude,
 codigo_mcc_id, 	 modelo_pagos, 	 fuente_creacion, 	 referido_cuenta_linea, 	 ciudad, 	 city, 	 plana_visa, 	 porcentual_visa, 	 plana_mc, 	 porcentual_mc, 	 plana_pse, 	 porcentual_pse, 	 plana_efecty, 	 porcentual_efecty, 	 recencia, 	 alt_baj_recencia, 	 frecuencia_internet, 	 alt_baj_frecuencia, 	 monto, 	 alt_baj_monto, 	 rfm_group, 	 estado_
into churn_flag_corregido
 from churn_flag 
 --limit 50
 ;


drop table if exists churn_flag_corregido_cast;
select
distinct
nombre_merchant,
mes,
inactividad
, cast (tpt as double precision)
, cast (tpv_usd as double precision)
, cast (revenue_usd as double precision)
, cast (gm_usd as double precision)
, cast (credit_card as bigint)
, cast (bank_referenced as bigint)
, cast (referenced as bigint)
, cast (bank_transfer as bigint)
, cast (pse as bigint)
, cast (debit_card as bigint)
, cast (cash as bigint)
, cast (cash_on_delivery as bigint)
, cast (ach as bigint)
, cast (lending as bigint)
, cast (suma_de_metodos as bigint)
, cast (inactivo as integer)
, cast (fecha_creacion_nombre as date)
, cast (primera_trx_nombre as date)
, cast (creacion_hasta_first_trx as bigint)
, cast (mob_first_trx as bigint)
, cast (mob_creacion_cuenta as bigint)
, cast (created_activated as character varying(17))
, cast (mes_anterior as date)
, cast (back_from_churn as character varying(7))
, cast (saldo_promedio_mes as numeric(38,18))
, cast (saldo_f_mes as numeric(38,18))
, cast (monto_fraude as numeric(38,18))
, cast (cantidad_fraude as bigint)
, codigo_mcc_id,
modelo_pagos,
fuente_creacion,
case when referido_cuenta_linea is not null then 'No' else 'Yes' end as referido_cuenta_linea,
ciudad,
city,
plana_visa,
porcentual_visa,
plana_mc,
porcentual_mc,
plana_pse,
porcentual_pse,
plana_efecty,
porcentual_efecty,
--recencia,
--alt_baj_recencia,
--frecuencia_internet,
--alt_baj_frecuencia,
--monto,
--alt_baj_monto,
--rfm_group,
estado_
into churn_flag_corregido_cast
from churn_flag_corregido
--limit 5
;

--union_activos_churn
drop table if exists union_activos_churn;
select *
into union_activos_churn
from 
(select *
from Activos_flag
union
select *
from churn_flag_corregido_cast) 
;
-------------buscar
-- nuevas creadas-------------------------------------------------
---------------------------------------
drop table if exists dim_account_nombres;
select distinct
case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t1.nombre end as nombre_merchant
, t1.cuenta_id  
, t1.usuario_id  
, t1.fecha_creacion  
, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion
, case when  t1.referido_cuenta_linea is null then 'No' else 'Yes' end as Referidot
,  t1.ciudad
into dim_account_nombres
from staging.polv4_pps_imp_cuenta   t1
left join client_base_nosotros t2 on t1.cuenta_id  = t2.account_id
--left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id 
inner join cruce_smb t3 on t1.cuenta_id = t3.account_id
where /*t1.cuenta_id in (select cuenta_id from cruce_smb) and*/ t1.cuenta_id not in (1, 2, 5, 501159, 515992,592315) 
;





drop table if exists creados_nombre_cuenta_ciudad;
select distinct *, case when ciudad ilike '%ogot%' then 'Bogotá and surroundings'
when ciudad ilike '%Medell%' then 'Medellín and surroundings'
when ciudad ilike '%cali' then 'Cali'
when ciudad ilike '%cartagen%' then 'Cartagena'
when ciudad ilike '%envigado%' then 'Medellín and surroundings'
when ciudad ilike '%itag%' then 'Medellín and surroundings'
when ciudad ilike '%barranq%' then 'Barranquilla'
when ciudad ilike '%bucarama%' then 'Bucaramanga'
when ciudad ilike '%chía%' then 'Bogotá and surroundings'
when ciudad ilike '%chia%' and ciudad <> 'QUINCHIA' then 'Bogotá and surroundings'
else 'Others' end as City
into creados_nombre_cuenta_ciudad
from dim_account_nombres
;




-- añadiendo medio de pago
drop table if exists creados_nombre_comision;--drop table if exists activos_nombre_mcc;
select distinct t8.*
	, t7.Plana_Visa, t7.Porcentual_Visa
	, t7.Plana_MC, t7.Porcentual_MC
	, t7.Plana_PSE, t7.Porcentual_PSE
	, t7.Plana_Efecty, t7.Porcentual_Efecty
into creados_nombre_comision--into activos_nombre_mcc
from creados_nombre_cuenta_ciudad t8
left join (
	select t5.nombre_merchant, t5.account_id
	, t111.comision_plana_comercio as Plana_Visa, t111.comision_porcentual_comercio as Porcentual_Visa
	, t222.comision_plana_comercio as Plana_MC, t222.comision_porcentual_comercio as Porcentual_MC
	, t333.comision_plana_comercio as Plana_PSE, t333.comision_porcentual_comercio as Porcentual_PSE
	, t444.comision_plana_comercio as Plana_Efecty, t444.comision_porcentual_comercio as Porcentual_Efecty
	from (
		select nombre_merchant, account_id, tpt_total
		, case when lag(nombre_merchant, 1) over(order by nombre_merchant, tpt_total desc) = nombre_merchant then
		0 else 1 end as lag_max
		from (
			select distinct
			case when t2.name_merchant_2 is not null then t2.name_merchant_2 else t3.nombre end as nombre_merchant
			, t1.account_id, t1.tpt_total 
			from 
				(select account_id, /*date(date_trunc('month',"month")) as mes ,*/ sum(tpt) as tpt_total
				from reports.fz1_client_base  
				where account_id in (select cast(account_id as varchar) as account_id  from cruce_smb) 
				and account_id not in (1, 2, 5, 501159, 515992,592315)
				group by account_id) t1
			left join client_base_nosotros t2 on t1.account_id  = t2.account_id
			left join staging.polv4_pps_imp_cuenta t3 on t1.account_id = t3.cuenta_id ) t4
			) t5
	left join staging.polv4_pps_imp_cuenta t6 on t5.account_id = t6.cuenta_id and cuenta_id 
	in  (select cast(account_id as varchar) as account_id  from cruce_smb)
	left join tarifas_x_cuenta t111 on t111.cuenta_id = t6.cuenta_id 
		and t111.tipo_medio_pago = 'VISA' 
	left join tarifas_x_cuenta t222 on t222.cuenta_id = t6.cuenta_id 
		and t222.tipo_medio_pago = 'MASTERCARD' 
	left join tarifas_x_cuenta t333 on t333.cuenta_id = t6.cuenta_id 
		and t333.tipo_medio_pago = 'PSE' 
	left join tarifas_x_cuenta t444 on t444.cuenta_id = t6.cuenta_id 
		and t444.tipo_medio_pago = 'EFECTY'
	where lag_max =1) 
	t7 on t8.nombre_merchant = t7.nombre_merchant --and t8.mes = date(date_trunc('month',t7.fecha_creacion))
order by nombre_merchant--, mes 	
;



--- agrupadas creadas
drop table if exists creadas_mes_variables;
select 
nombre_merchant, date(date_trunc('month', min(fecha_creacion))) as mes, codigo_mcc_id
, modelo_pagos, fuente_creacion, referidot, ciudad, city, plana_visa
, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
into creadas_mes_variables
from creados_nombre_comision
group by nombre_merchant, codigo_mcc_id
, modelo_pagos, fuente_creacion, referidot, ciudad, city, plana_visa
, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty
;



-- corrigiendo las creadas
drop table if exists creadas_flag ;
select *, 'Creada' as Estado_ 
into Creadas_flag
from creadas_mes_variables
--limit 5
;



drop table if exists creadas_flag_corregida;
select 
nombre_merchant,	 mes
,null as  inactividad,	null as  tpt,	null as  tpv_usd,	null as  revenue_usd,	null as  gm_usd,	null as  credit_card,	null as  bank_referenced,	null as  referenced,	null as  bank_transfer,	null as  pse,	null as  debit_card,	null as  cash,	null as  cash_on_delivery,	null as  ach,	null as  lending,	null as  suma_de_metodos,	null as  inactivo,	null as  fecha_creacion_nombre,	null as  primera_trx_nombre,	null as  creacion_hasta_first_trx,	null as  mob_first_trx,	null as  mob_creacion_cuenta,	null as  created_activated,	null as  mes_anterior,	null as  back_from_churn,	null as  saldo_promedio_mes,	null as  saldo_f_mes,	null as  monto_fraude,	null as  cantidad_fraude,
 codigo_mcc_id,	 modelo_pagos,	 fuente_creacion,	 referidot,	 ciudad,	 city,	 plana_visa,	 porcentual_visa,	 plana_mc,	 porcentual_mc,	 plana_pse,	 porcentual_pse,	 plana_efecty,	 porcentual_efecty,
null as  recencia, 	null as  alt_baj_recencia, 	null as  frecuencia_internet, 	null as  alt_baj_frecuencia, 	null as  monto, 	null as  alt_baj_monto, 	null as  rfm_group, 
 estado_
 into creadas_flag_corregida
 from Creadas_flag
 ;


drop table if exists creadas_flag_corregida_cast;
select distinct nombre_merchant,
mes,
cast(inactividad as bigint ),
cast(tpt as double precision ),
cast(tpv_usd as double precision ),
cast(revenue_usd as double precision ),
cast(gm_usd as double precision ),
cast(credit_card as bigint ),
cast(bank_referenced as bigint ),
cast(referenced as bigint ),
cast(bank_transfer as bigint ),
cast(pse as bigint ),
cast(debit_card as bigint ),
cast(cash as bigint ),
cast(cash_on_delivery as bigint ),
cast(ach as bigint ),
cast(lending as bigint ),
cast(suma_de_metodos as bigint ),
cast(inactivo as integer ),
cast(fecha_creacion_nombre as date ),
cast(primera_trx_nombre as date ),
cast(creacion_hasta_first_trx as bigint ),
cast(mob_first_trx as bigint ),
cast(mob_creacion_cuenta as bigint ),
cast(created_activated as character varying(17) ),
cast(mes_anterior as date ),
cast(back_from_churn as character varying(7) ),
cast(saldo_promedio_mes as numeric(38,18) ),
cast(saldo_f_mes as numeric(38,18) ),
cast(monto_fraude as numeric(38,18) ),
cast(cantidad_fraude as bigint ),
codigo_mcc_id,
modelo_pagos,
fuente_creacion,
case when referidot is not null then 'No' else 'Yes' end as referidot,
ciudad,
city,
plana_visa,
porcentual_visa,
plana_mc,
porcentual_mc,
plana_pse,
porcentual_pse,
plana_efecty,
porcentual_efecty,
/*cast(recencia as bigint ),
cast(alt_baj_recencia as character varying(4) ),
cast(frecuencia_internet as bigint ),
cast(alt_baj_frecuencia as character varying(4) ),
cast(monto as numeric(38,4) ),
cast(alt_baj_monto as character varying(4) ),
cast(rfm_group as character varying(11) ),*/
cast(estado_ as character varying(7) )
into creadas_flag_corregida_cast
from creadas_flag_corregida
--limit 5
;


/*select pg_get_cols('Activos_flag')
;
select pg_get_cols('creadas_flag_corregida')
;*/


drop table if exists nombre_mes_Creadas;
select nombre_merchant, mes, 1 as flag
into nombre_mes_Creadas
from activos_nombre_rfm
where created_activated = 'activa_mes_creada'
;


drop table if exists creadas_flag_pre_union; 
select t1.*, t2.flag
into creadas_flag_pre_union
from  creadas_flag_corregida_cast t1 --on t1.nombre_merchant = t2.nombre_merchant and t1.mes = t2.mes
left join nombre_mes_Creadas t2 on t1.nombre_merchant = t2.nombre_merchant  and t1.mes = t2.mes 
--limit 5
;





drop table if exists creadas_vip_upgrade;
select t1.*, case when t1.nombre_merchant in ('20/20 BIENES & SERVICIOS SAS',
	'3 GRACIAS SAS',
	'3D JUNQUERA FONT SL',
	'A MODO MIO SAS',
	'A2 HOSTING, INC.',
	'ABECEDE SAS',
	'AC COLOMBIA LAWYERS',
	'ACADEMIA DE ARTES GUERRERO',
	'ACERSHOES LTDA',
	'ACOLCHADOS EDREDONA LTDA',
	'ACSENDO S.A',
	'ACTIVE BRANDS SAS',
	'ACTUALICESE.COM LTDA',
	'ADALBERTO ANTONIO HERRERA CUELLO',
	'ADWELLCH',
	'AEROEJECUTIVOS DE ANTIOQUIA SA',
	'AG ESTUDIO SAS',
	'AGENDAPRO COLOMBIA SAS',
	'AGILEX EXPRESS S.A.S',
	'ALCANOS DE COLOMBIA S.A. ESP',
	'ALEJANDRA PATRICIA ECHAVARRIA HUTTA',
	'ALEJANDRO LOYNAZ',
	'ALEJANDRO YARCE VILLA',
	'ALFRED SAS',
	'ALIANZA BSH SAS',
	'ALIMENTOS TONING S.A',
	'ALMACENES BYBLA SAS',
	'ALMACENES CORONA S.A.S',
	'ALMACENES LA BODEGA S A',
	'ALQUIMIA DE LIBRA SAS',
	'ALTA TECNOLOGIA EN EL APRENDIZAJE S.A',
	'ALTER EGO SAS',
	'ALTIPAL',
	'ALVARO ESCANDON',
	'ALVARO LUQUE',
	'AMERICA DE CALI SA',
	'ANA ISABEL SANTA MARIA TOBON',
	'ANA MARIA GALLEGO',
	'ANA MARIA VELEZ BETANCUR',
	'ANALYTICS ACADEMY S.A.S',
	'ANCANA SAS',
	'ANDESCO',
	'ANDINOEXPRESS SL',
	'ANDREA MILANO 1932 SAS',
	'ANDREA REGGIO',
	'ANDRES FELIPE RAMIREZ RENDON',
	'ANGELA MARIA SOLARTE CRUZ',
	'ANGELICA RODRIGUEZ',
	'APASIONA T SAS',
	'APRENDER SAS',
	'AQUAROSA SAS',
	'ARQUESOFT SAS',
	'ASOCIACION COLEGIO CRISTIANO J',
	'ASOCIACION COLOMBIANA DE OTORRINOLARINGOLOGIA',
	'ASOCIACION COLOMBIANA DE PROFESORES DE FRANCES',
	'ASOCIACION DE ARTESANAS DE CHORRERA',
	'ASOCIACION NACIONAL DE INTERNOS Y RESIDENTES',
	'ASOCIACION PROFESIONAL IEEE COLOMBIA SP',
	'ASOCIACION SIN ANIMO DE LUCRO EO COLOMBIA',
	'ASSA ABLOY COLOMBIA',
	'ATC COLOMBIA SAS',
	'ATENEA ENTERPRISE SAS',
	'ATHLETIC COLOMBIA S A',
	'AUDIOCOM SAS',
	'AUREA VANESSA GARCIA',
	'AUTOELITE LTDA',
	'AUTORINES Y LLANTAS LTDA',
	'AVANTIKA COLOMBIA',
	'AVENU LEARNING LLC',
	'AYCI',
	'BALUARTE ESTUDIO JURIDICO SAS',
	'BEAUTY AND COSMETICS SAS',
	'BENDITA SEAS ROPA Y ACCESORIOS SAS',
	'BETTINA SPITZ Y CIA LTDA.',
	'BIKE GIRLS S.A.S',
	'BMC BOLIVAR SAS',
	'BMFN SAS',
	'BONITAS',
	'BONOGIFT S.A.S.',
	'BOTANIQUE S.A.S',
	'BOUTIQUE DE LA INDIA S.A.S',
	'BOY TOYS FACTORY SAS',
	'BRANDLIVE - SAMSONITE',
	'BUCCO CYCLING SAS',
	'BUENAHORA SAS',
	'BUREAU VERITAS COLOMBIA LTDA',
	'CACAO DE COLOMBIA SAS',
	'CACHARRERIA MUNDIAL',
	'CALZADOS AZALEIA',
	'CAMARA DE COMERCIO DE CALI',
	'CAMILA MONTOYA PALACIO',
	'CAMILO ARBELAEZ GOMEZ',
	'CAMINOWEB SAS',
	'CARIBE INTERCOMUNICACIONES SAS',
	'CARLOS ALBERTO TABARES GUTIERREZ',
	'CARMEN MARQUEZ',
	'CAROLINA GRILLO TARCHOPULOS',
	'CAROLINA LOZANO',
	'CAROLINA MANRIQUE TEJEDOR',
	'CARTAGENA BOAT CHARTERS SAS',
	'CARVAJAL ESPACIOS S.A.S.',
	'CASA FECHI SAS',
	'CASA SOBRE LA ROCA IGLESIA CRISTIANA INTEGRAL',
	'CASTOR MUEBLES Y ACCESORIOS SAS',
	'CATALINA MARTINEZ HERNANDEZ',
	'CATEDRAL DE SAL ZIPAQUIRA SA',
	'CAVIEDEZ Y ASOCIADOS SAS',
	'CCF CUP & CAKE FACTORY SAS',
	'CCTI SAS',
	'CENTRAL COMERCIALIZADORA',
	'CENTRO CANINO Y VETERINARIO OCHO PERROS SAS',
	'CENTRO DE DE INVESTIGACION CLINICA COLOMBIANA OBESIDAD Y METABOLISMO SAS',
	'CENTRO DE ESTUDIOS SOCIO JURIDICOS LATINOAMERICANO',
	'CENTRO DE EVENTOS ROYAL CENTER',
	'CENTRO DE EVENTOS ROYAL CENTER SAS',
	'CENTRO DE GESTION HOSPITALARIA',
	'CENTRO DE IDIOMAS ASIATICOS',
	'CENTRO PARA EL DESARROLLO INFANTIL HUELLAS SAS',
	'CENTROS DE LITERATURA CRISTIANA DE COLOMBIA C.L.C NULL',
	'CHALLENGER SAS',
	'CHEVY PLAN',
	'CHIC MARROQUINERIA LTDA',
	'CHIMENEAS DE COLOMBIA INGENIEROS ARQUITECTOS SAS',
	'CI COLOR SIETE SAS',
	'CI MIGUEL CABALLERO SAS',
	'CI SAN JOSE FARMS LTDA',
	'CIUDADDEMASCOTAS.COM SAS',
	'CLAN GADFER S.A.S',
	'CLARISSA ROSANIA SAS',
	'CLASSIC JEANS SHOP S.A.S.',
	'CLB DEPORTIVO TEAM LA CICLERIA',
	'CLERIGOS DE SAN VIATOR',
	'CLINICAS ODONTOLOGICAS DR JIMMER HERNANDEZ SAS',
	'CLOUD MEDIA SAS',
	'CLUB DEL PASEO SAS',
	'CLUB DEPORTIVO CATERPILLAR MOTOR',
	'CLUB DEPORTIVO FREDY GONZALEZ',
	'CLUB DEPORTIVO MARACANEIROS',
	'CO INTERNET',
	'COCORA INSTITUTE SAS',
	'COL HOGAR 1 SAS',
	'COLECTIVO AQUI Y AHORA SAS',
	'COLEGIO COLOMBIANO DE FISIOTERAPEUTAS COLFI',
	'COLEGIO SAN JUAN DE LOS PASTOS EU',
	'COLOMBIAN GYMWEAR',
	'COMERCIAL CARDONA HNOS. LTDA',
	'COMERCIALIZADORA BORA S.A.S',
	'COMERCIALIZADORA HERREROS SAS',
	'COMERCIALIZADORA INTERNACIONAL COCOLMEX SAS',
	'COMERCIALIZADORA INTERNACIONAL GRUPO DE MODA S A S',
	'COMFACESAR',
	'COMFATOLIMA',
	'COMPAÑIA COLOMBIANA DE CERAMICA S.A.S',
	'COMPAÑIA DE LOS MUEBLES SAS',
	'COMPAÑIA MANUFACTURERA MANISOL S.A.',
	'COMPETENTUM GROUP S.A.S.',
	'COMUNICATE FACIL COLOMBIA S.A.S.',
	'CONFECCIONES BRAVASS SAS',
	'CONFECCIONES SALOME LTDA',
	'CONGO FILMS SAS',
	'CONSEJO PROFESIONAL DE QUIMICA',
	'CONSORCIO DE CONFECCIONES S.A.',
	'CONSUMER ELECTRONIC SAS',
	'COOPERATIVA MULTIACTIVA DE LOS TRABAJADORES DE SANTANDER',
	'CORPARQUES',
	'CORPORACION AMBIENTAL COLOMBIA RESERVA DE VIDA',
	'CORPORACION AUTONOMA REGIONAL DEL VALLE DEL CAUCA',
	'COSMETIKAS SAS',
	'CREACIONES ALMAMIA SAS',
	'CRIADERO LA MARQUEZA',
	'CRISTALERIA SELMAN SAS',
	'CRISTIAN ESCOBAR MAHECHA',
	'CROYDON COLOMBIA S.A.',
	'CUBOCAMP SAS',
	'DANIEL RICARDO RODRIGUEZ CAMARGO',
	'DANIELA ARANGO',
	'DANIELA BERRIO',
	'DANIELA MARTINEZ',
	'DANIELA RUIZ',
	'DAPAC SERVICES SAS',
	'DATATTEC COLOMBIA SAS',
	'DEKO UÑAS SAS',
	'DELASIEMBRA COM SAS',
	'DENIS AMADO',
	'DEPLANTA',
	'DERMOSALUD S.A.',
	'DESINGS GROUP SAS',
	'DEVAG INGENIERIA SAS',
	'DIANA LUZ RAMIREZ CASTAÑO',
	'DIANA MARCELA CHICA SANCHEZ',
	'DIANA SERNA SERNA HERNANDEZ',
	'DIANA SOFIA POLO OSPINO',
	'DIGITAL INVESTMENT GROUP SAS',
	'DINO ANDRUS RENTERIA MONTENEGRO',
	'DISNEYLANDIA DISEÑOS S A S',
	'DISTRIBEAUTE COLOMBIA SAS',
	'DISTRIBUIDORA EL DIVINO SAS',
	'DISTRIBUNA LTDA',
	'DISTRICATOLICAS UNIDAS SAS',
	'DOMINIO AMIGO S.A.S',
	'DOMUS GLAMPING SAS',
	'DPFRATELLI SAS',
	'DULCE MENTA S.A.S.',
	'E DISTRIBUTION SAS',
	'ECOMOTORES INTERNACIONAL SAS',
	'EDITORA TKE LEARNING S.A.S.',
	'EDITORIAL ITA SAS',
	'EDITORIAL TEMIS S.A.',
	'EDUARDO JOSE CANTILLO ANGULO',
	'EDUFIRST',
	'EF SERVICIOS EN EDUCACION INTERNACIONAL SA DE CV',
	'EG RETAIL SAS',
	'EKONOMODO COLOMBIA SAS',
	'EL JARRO PRODUCTOS ALIMENTICIOS SAS',
	'ELECTROBIKE COLOMBIA SAS',
	'ELECTROFERIA DE LA CARRERA 13 SAS',
	'ELITE TRAINING LTDA',
	'ELYTE ELECTRONICA Y TELECOMUNICACIONES LTDA',
	'EMPOWER HOLDING SAS',
	'EMPRESA COLOMBIANA DE CEMENTOS S.A.S',
	'EMPRESA DE ACUEDUCTO ALCANTARILLADO Y ASEO DE MADRID ESP',
	'EN TELA LTDA',
	'ENTER SITE LTDA',
	'EQUILIBRIO SAS',
	'ERGONOMUS S.A.S',
	'ESCUELA DE NEGOCIOS CONSCIENTES S.A.S',
	'ESTEFANIA CASTRO SUAREZ',
	'ESTRATEK SAS',
	'ETNIKO SAS',
	'EUGENIA FERNANDEZ SAS',
	'EUROINNOVA FORMACION S.L.',
	'EW TECH SAS',
	'FABIANA CAMILA SANCHEZ HERNANDEZ',
	'FABY SPORT S.A.S.',
	'FAJAS PIEL DE ANGEL SAS',
	'FARMACIA DERMA LIFE',
	'FARMAPATICA COLOMBIA S.A.S',
	'FENOMENA SAS',
	'FERNEY ANTONIO PARADA CHASOY',
	'FERRIMAQ DE COLOMBIA SAS',
	'FIBER SAS',
	'FLOR ALBA CANTE AREVALO',
	'FLOR MARIA REYES PAEZ',
	'FLORISTERIA HOJAS BLANCAS S.A.S',
	'FORK CATERING AND EVENT PLANNING SAS',
	'FORUM MEDIA POLSKA SP. Z.O.O',
	'FOUR BROTHERS S.A.S.',
	'FUCSIA BOUTIQUE',
	'FUERZA GASTRONOMICA SAS',
	'FUNDACION CASATINTA COLOMBIA',
	'FUNDACION CIENTIFICA LHA',
	'FUNDACION CIGARRA',
	'FUNDACION CULTURAL ASIA IBEROAMERICA',
	'FUNDACION EL QUINTO ELEMENTO',
	'FUNDACION HOGAR NUEVA GRANADA',
	'FUNDACION INFANTIL SANTIAGO CORAZON',
	'FUNDACION JUAN FELIPE GOMEZ ESCOBAR',
	'FUNDACION MADAURA',
	'FUNDACION RED AGROECOLOGICA LA CANASTA',
	'FUNDACION UNIVERSITARIA DEL AREA ANDINA',
	'FUNERARIA Y FLORISTERIA LOS ANGELES SA',
	'GABRIEL GARZON',
	'GALAN Y CIA S. EN C. AMERICAN CHEESE CAKES',
	'GALERIA CAFE LIBRO CLUB SOCIAL PRIVADO SA',
	'GAMBOA INSTUDIO SAS',
	'GENERAL DE EQUIPOS DE COLOMBIA SA GECOLSA',
	'GERMAN ALEXANDER FRANCO BENITEZ',
	'GESCONS SAS',
	'GIGAPALMAR S.A.S.',
	'GIVELO SAS',
	'GLOBAL BLUE HYDROS',
	'GM ONLINE SAS',
	'GOLDEN WOLF TRADING SAS',
	'GOLF Y TURF SAS',
	'GRATU COLOMBIA SAS',
	'GREEN GATEWAY S.A.S',
	'GREEN GLASS COLOMBIA',
	'GRUPO ASTHEC S.A',
	'GRUPO BIROTA SAS',
	'GRUPO COLORS EQUIPOS SAS',
	'GRUPO DOX',
	'GRUPO EDITORIAL MUNDO NIÑOS',
	'GRUPO ELITE FINCA RAIZ',
	'GRUPO EMPRESARIAL AM SAS',
	'GRUPO EMPRESARIAL GEIN',
	'GRUPO EMPRESARIAL MDP',
	'GRUPO EPACHAMO SAS',
	'GRUPO FORSE SAS',
	'GRUPO HERFAL SAS',
	'GRUPO JULIO DE COLOMBIA SAS',
	'GRUPO MIS SAS',
	'GRUPO PHOENIX - MULTIDIMENSIONALES S.A.S',
	'GRUPO QUINCENA S.A.S',
	'GRUPO RENTAWEB SAS',
	'GRUPO SLAM SAS',
	'GRUPO URDA SAS',
	'GRUPO VAS A VIAJAR SAS',
	'GRUPO WONDER SA',
	'GUARNIZO Y LIZARRALDE SAS',
	'GUILLERMO PERILLA Y CIA LTDA',
	'GVS10 LTDA',
	'H3S',
	'HALCON TECNOLOGIA SAS',
	'HAPPY EUREKA SAS',
	'HCML COLOMBIA SAS',
	'HEALTHY AMERICA COLOMBIA SAS',
	'HECHO EN TURQUIA SAS',
	'HENAO PUGLIESE S A S',
	'HIPNOS BRAND SAS',
	'HOBBY CON',
	'HOJISTICA LIMITADA',
	'HOSTING RED LTDA',
	'HP COLOMBIA S.A.',
	'I LOVE GROUP SAS',
	'ICONIC STORE SAS',
	'IFRS MASTERS COLOMBIA SAS',
	'IGLESIA CRISTIANA EMANUEL - DIOS CON NOSOTROS',
	'IGLESIA MISION CARISMATICA AL MUNDO',
	'IGLESIA MISION PAZ A LAS NACIONES',
	'IMPORTADORA GORR SAS',
	'IMPORWORL SAS',
	'IN GRILL SAS',
	'IN OTHER WORDS S.A',
	'INADE SAS',
	'INDUSTRIA DE GALLETAS GRECO',
	'INDUSTRIA DE MUEBLES DEL VALLE INVAL S.A',
	'INDUSTRIAS DE ALIMENTOS CHOCONATO SAS',
	'INDUSTRIAS RAMBLER S.A.S.',
	'INDUSTRIES BERAKA EFRATA SAS',
	'INFINITY NETWORKS SAS',
	'INFLAPARQUE ACUATICO IKARUS SAS',
	'INFORMA COLOMBIA SA',
	'INGENIERIA ASISTIDA POR COMPUTADOR S.A.S',
	'INSTITUTO DE MEDICINA FUNCIONAL CJ',
	'INSUMOS Y AGREGADOS DE COLOMBIA S.A.S.',
	'INTERCLASE SRL',
	'INTERNATIONAL EXPORT BUREAU SAS',
	'INTRA MAR SHIPPING SAS',
	'INVERLEOKA S.A.S',
	'INVERLEOKA SAS',
	'INVERNATURA SAS',
	'INVERSIONES CAMPO ISLEÑO S.A',
	'INVERSIONES CORREA RUIZ LTDA',
	'INVERSIONES DERANGO SAS',
	'INVERSIONES EL CARNAL SAS',
	'INVERSIONES FLOR DE LIZ LTDA',
	'INVERSIONES MEDINA TRUJILLO',
	'INVERSIONES MERPES',
	'INVERSIONES MI TIERRITA SAS',
	'INVERSIONES MPDS SAS',
	'INVERSIONES SECURITY SAS',
	'INVERSIONES TINTIPAN SAS',
	'INVERSIONES TITI SAS',
	'IT CLOUD SERVICES',
	'JAIME',
	'JAIRO ALEXANDER CASTILLO LAMY',
	'JAVIER ALEXANDER NIETO RAMIREZ',
	'JAVIER ARMANDO ORTIZ LOPEZ',
	'JEANS MODA EXPORTACIONES',
	'JEISON VARGAS',
	'JENNY PATRICIA PLAZAS ANGEL',
	'JERUEDACIA',
	'JHON CRUZ GOMEZ',
	'JOHANNA BEATRIZ REYES GARCIA',
	'JORGE ALBERTO GUERRA CARDONA',
	'JORGE SANCHEZ',
	'JOSE A DELGADO',
	'JOSE ALEJANDRO VARGAS ANGEL',
	'JOSE URIEL TORO MANRIQUE',
	'JOSERRAGO SA',
	'JTG TORRALBA HERMANOS S EN C',
	'JUAN CASABIANCA',
	'JUAN DIEGO GOMEZ',
	'JUAN ESTEBAN CONSTAIN CROCE',
	'JUAN ESTEBAN PELAEZ GOMEZ',
	'JUAN FELIPE POSADA LONDOÑO',
	'JUAN GUILLERMO GARCES RESTREPO',
	'JUAN IGNACIO GOMEZ CORREA',
	'JUAN MIGUEL MESA RICO',
	'JUANA FRANCISCA S.A',
	'JULIAN EDUARDO ZAMORA PRIETO',
	'JULIAN GABRIEL FLOREZ ROSALES',
	'JULIANA MARIA MATIZ VEGA',
	'JULIETTE DAHIANA CASTILLO MORALES',
	'JUVENIA S.A',
	'KASA WHOLEFOODS COMPANY SAS',
	'KATHERINE ALZATE',
	'KEMISER SAS',
	'KFIR COLOMBIA S.A.S',
	'KHIRON COLOMBIA SAS',
	'KIBYS S.A.S',
	'KYDOS S.A.',
	'KYVA SAS',
	'L ATELIER DESSERT',
	'LA CARPINTERIA ARTESANAL 2016 SAS',
	'LA MAR SENSUAL SHOP SAS',
	'LABORATORIO JV URIBE M LTDA',
	'LABORATORIOS ATP SAS',
	'LABORATORIOS LEGRAND SA',
	'LABRIUT SAS',
	'LAGOBO DISTRIBUCIONES S A L G B S A',
	'LAURA ESCOBAR',
	'LAURA ROMERO',
	'LCN IDIOMAS SAS',
	'LEIDY JOHANA CRUZ',
	'LIBRERIA MEDICA CELSUS LTDA',
	'LICEO CRISTIANO VIDA NUEVA',
	'LIFETECH SAS',
	'LIGHT DE COLOMBIA S A',
	'LILIANA MORENO CASAS',
	'LILIANA PAOLA ORTIZ REYES',
	'LINK DIAZ SAS',
	'LISBEISY CAROLINA DIAZ RAMOS',
	'LIVA SOLUCION EN COMUNICACIONES',
	'LOCERIA COLOMBIANA SAS',
	'LOGISTICA Y DISTRIBUCIONES ONLINE LTDA',
	'LONJA DE COLOMBIA',
	'LORENA CUERVO DIAZ SAS',
	'LOSMILLONARIOSNET SAS',
	'LOVERA U SAS',
	'LTM3 SAS',
	'LUCAS BRAVO REYES',
	'LUIS ACONCHA',
	'LUIS DARIO BOTERO GOMEZ',
	'LUIS ENRIQUE GOMEZ DE LOS RIOS',
	'LUIS FERNANDO LOPEZ VELASQUEZ',
	'LUIS IBARDO MORALES ARIAS',
	'LUISA FERNANDA LIEVANO GARCIA',
	'LULETA SAS',
	'MAH! COLOMBIA S.A.S.',
	'MAIRA ALEJANDRA GOMEZ FONSECA',
	'MAKLIK TECHNOLOGIES SAS',
	'MANDALAS PARA EL ALMA S.A.S',
	'MANTELTEX SAS',
	'MANTIS GROUP SAS',
	'MANUFACTURAS A F SAS',
	'MANUFACTURAS KARACE SAS',
	'MAQUI SPORTSWEAR',
	'MAR BY MARISELA MONTES',
	'MAR DEL SUR LTDA',
	'MARIA ALEJANDRA PATIÑO',
	'MARIA CAMILA OCHOA NEGRETE',
	'MARIA CLAUDIA BARRIOS MENDIVIL',
	'MARIA DEL ROSARIO URIBE',
	'MARIA FERNANDA VALENCIA',
	'MARIA ISABEL JARAMILLO DIAZ',
	'MARIO ALFONSO MONTOYA PAZ',
	'MASCOTAS MAR SAS',
	'MASTER LCTL SAS',
	'MATERIALES ELECTRICOS Y MECANICOS SAS',
	'MAYRA LISSETTE GOMEZ PARRA',
	'MC DEVINS SAS',
	'MC GRAW HILL INTERAMERICANA EDITORES SA',
	'MDALATAM SAS',
	'MELOPONGO.COM.S.A.S',
	'MERCADO & PLAZA S.A.S.',
	'MERCADO COMUN SAS',
	'MERCADO VITAL SAS',
	'MERCANTE SAS',
	'MERCAVIVA COL SAS',
	'MERCEDES PIRAZA ISMARE',
	'MERIDIAN GAMING COLOMBIA',
	'MESSER COLOMBIA SA',
	'MEYPAC SAS',
	'MILENA VELASQUEZ',
	'MISIMCARDCOM',
	'MODA ACTUAL LTDA',
	'MODA OXFORD SA',
	'MODAS CLIO SAS',
	'MODERI SAS',
	'MOISES LONDONO',
	'MONASTERY COUTURE SAS',
	'MONTOC',
	'MOTOS Y ACCESORIOS SAS',
	'MOVITRONIC',
	'MUEBLES FABRICAS UNIDAS SAS',
	'MULTIAUDIO PRO LTDA',
	'MULTIMEDICO SAS',
	'MUNDIAL S.A.S',
	'MUNDIENLACE EN CONTACTO SAS',
	'MUTANTEST',
	'NAMASTE DESIGN SAS',
	'NATALIA ALTAHONA',
	'NATALIA BOTERO TORO',
	'NATALIA GONZALEZ',
	'NATURA ANAPOIMA RESERVADO EMPRESARIOS FENIX SAS',
	'NATURAL LABEL SAS',
	'NATY LONDON SAS',
	'NESS WELL S.A.S',
	'NESTOR ZULUAGA GOMEZ',
	'NETSHOP FULFILLMENT S.A.S.',
	'NICOLAS VASQUEZ',
	'NIHAO COLOMBIA SAS',
	'NINFER BETANCOURT',
	'NMV COLOMBIA S.A.S',
	'NON STOP ENTERTAINMENT SAS',
	'NUBIA CARDENAS SPA SAS',
	'NUESTRA COCINA ARTESANAL',
	'NURY CATALINA MENDIETA DIAZ',
	'NUTRABIOTICS SAS',
	'OFICOMPUTO LTDA',
	'OLFABRAND NATURAL WELLNESS SAS',
	'OLGA CRISTINA FLOREZ HERRERA',
	'OPERADORA COLOMBIANA HOTELERA SAS',
	'OPERADORA MOCAWA PLAZA SAS',
	'OPORTUNIDAD FLASH COLOMBIA SAS',
	'OPTICA ALEMANA E Y H SCHMIDT S.A.',
	'OPTIMANT COLOMBIA SAS',
	'ORGANIZACION SERIN LTDA',
	'ORTOPEDICOS WILLIAMSON Y WILLIAMSON SAS',
	'OSCAR DAVID LARA ARTURO',
	'OSCAR EDUARDO OSPINA GUERRERO',
	'OSCAR ORTEGA FLORES',
	'OUR BAG SAS',
	'PADOVA SAS',
	'PAJAROLIMON S.A.S',
	'PANAMERICANA DE DISTRIBUCIONES GARRIDO SAS',
	'PARROQUIA LA MILAGROSA',
	'PASTELERIA SALUDABLE LIBRE DE CULPA SAS',
	'PATRICIA BRICEÑO',
	'PEARSON EDUCACION DE COLOMBIA S.A.S.',
	'PEEWAH SAS',
	'PERA DK ROPA SAS',
	'PERA DK S.A.S',
	'PERCOS S. A.',
	'PHILIPPE PASTELERIA SAS',
	'PINK SECRET VIP SAS',
	'PLASTICOS ASOCIADOS S.A.',
	'PLASTIGLASS SAS',
	'PLAYA KORALIA SAS',
	'PLENIMODA SAS',
	'POLITO',
	'POLO1 SAS',
	'PONTIFICIA UNIVERSIDAD JAVERIANA',
	'PRO10',
	'PRODALIA COL SAS',
	'PRODUCTORA Y COMERCIALIZADORA DE PREDAS INTIMAS SAS CO',
	'PRODUCTOS YUPI SAS',
	'PROFY SAS',
	'PROMEDICA NATURAL BIONERGETICA SIU TUTUAVA IPS S.A',
	'PROMOFORMAS S.A.S',
	'PROMOTORA INMOBILIARIA DANN',
	'PROMOTORA PICCOLO S.A.',
	'PROTEGE TU VIAJE S.A.',
	'PROVENSAS SAS',
	'PROVIDA',
	'PUAROT COLOMBIA SAS',
	'PUNTOS Y MERCADOS SAS',
	'PURA IMAGEN LTDA',
	'QENTA SAS',
	'QUEVEDO TORRES LTDA',
	'RAFAEL FRANCISCO ZUÑIGA',
	'RAFAEL MAURICIO NUÑEZ GARZON',
	'RAMIRO TORO GUARIN',
	'RAPIMERCAR LTDA',
	'RAYJAR SAS',
	'RCA & ASOCIADOS SAS',
	'REA SOLUCIONES SAS',
	'RED DE PEDAGOGAA SAS',
	'REDES ORION SAS',
	'REDFRED SAS',
	'REGION SIMPLIFICADO',
	'REIZEN SAS',
	'REMG INGENIERIA SAS',
	'RENA WARE DE COLOMBIA S.A',
	'REPRODUCCION ANIMAL BIOTECNOLOGICA',
	'RESEM COLOMBIA S.A.S',
	'RESERVA ONE LOVE SAS',
	'RESTAURANTE MUY SAS',
	'RETRO KNOB S.A.S.',
	'RFG REPRESENTACIONES SAS',
	'RFID TECNOLOGIA SAS',
	'RICARDO ANTONIO ORTEGA VILLEGAS',
	'RICARDO FRAILE ROJAS',
	'RICARDO RAMIREZ',
	'RICARDO SANTANA',
	'RICHARD ALEXANDER LUGO PIRAQUIVE',
	'RISKS INTERNATIONAL SAS',
	'RODRIGUEZ IGUARAN SAS',
	'ROGGER ADRIAN CARDONA LOPEZ',
	'ROJAS TRASTEOS SERVICIOS SA',
	'RONDA S.A',
	'ROYAL ELIM INTERNACIONAL SAS',
	'ROYAL SAS',
	'SALUD SEMILLAS PLATAFORME',
	'SANNUS FOODS SAS',
	'SANTA COSTILLA SAS',
	'SANTANA SAS',
	'SANTIAGO ANDRES MENDIETA PEREZ',
	'SANTIAGO ANDRES OSORIO ARBOLEDA',
	'SANTIAGO BOTERO',
	'SARA ECHEVERRI',
	'SARA FERNANDEZ GOMEZ',
	'SARA MARIA TOBO YEPES',
	'SARA RUA SIERRA',
	'SARAI CLOTHING S.A',
	'SATLOCK LOGISTICA Y SEGURIDAD SAS',
	'SATRACK INC DE COLOMBIA SERVISAT SAS',
	'SAVA OUTSORCING SAS ZOMAC',
	'SCHALLER DESIGN AND TECHNOLOGY SAS',
	'SCHWARTZ BUSINESS SOLUTIONS S.A.S',
	'SCOTCHLAND SAS',
	'SEBASTIAN CARDONA GIRALDO',
	'SEBASTIAN MONSALVE CORREA',
	'SELETTI SAS',
	'SEOUL MEDICINA ESTETICA INTEGRAL & SPA',
	'SERGIO Y ALEXANDRA RADA SAS',
	'SERPRO DIGITAL SAS',
	'SESAMOTEX SAS',
	'SETROC MOBILE GROUP SAS',
	'SHERYL SAIZ',
	'SHOEMASTERS S.A.S',
	'SI SAS',
	'SICK UNIFORMS',
	'SIEMBRAVIVA SAS',
	'SIESUA MEDICINA LASER Y SPA SAS',
	'SILVIA ALEJANDRA NUÑEZ GARZON',
	'SILVIA JULIANA ORTIZ',
	'SILVIA PAOLA NAVARRETE VENEGAS',
	'SIMONIZ SA',
	'SISTEMAS INTELIGENTES Y TECNOLOGIA S',
	'SISTEMAS MODULARES DE ALUMINIO SAS',
	'SKENA SAS',
	'SMILEFUL COLOMBIA S.A.S.',
	'SOBERANA SAS',
	'SOCIEDAD COMERCIAL ZAM LIMITADA',
	'SOCIEDAD PARA EL AVANCE DE LA PSICOTERAPIA CENTRAD',
	'SOCIEDAD PORTUARIA REGIONAL DE BUENAVENTURA S.A.',
	'SOCODA SAS',
	'SPORTCHECK SAS',
	'STARWEAR INTERNATIONAL S.A',
	'STIT SKINCARE AND BEAUTY SAS',
	'SU FABRICA DE VENTAS SAS',
	'SUEÑA Y CREA INVERSIONES SAS',
	'SUFIES EDUCACION SAS',
	'SUMINISTROS DACAR SAS',
	'SUMINISTROS DE COLOMBIA S.A.S.',
	'SUMMERHOUSE SAS',
	'SUPER DE',
	'SUPERMERCADO NATURISTA LTDA',
	'SUPERSIGNS SAS',
	'SURFERS INTERACTIVE SAS',
	'SUSANA MEJIA GAVIRIA',
	'SVELTHUS CLAIICA DE REJUVENECIMIENTO FACIAL Y COR',
	'SYB COLOMBIA S.A.S',
	'TABA SPORT S.A.S.',
	'TALLER ALVAREZ VILLA SAS',
	'TE DELUXE GROUP S.A.S',
	'TEAM FLOWERS COLOMBIA',
	'TECNIFACIL SAS',
	'TECNOPTIX SAS',
	'TELEACCION SAS',
	'TERRAMAGA SAS',
	'TEXTILES SWANTEX S A',
	'TEXTILES VELANEX S.A',
	'TEXTILES VMG LTDA',
	'TEXTRON',
	'THE DREAMER OPERATIONS SAS',
	'THUNDERBOLT SAS',
	'TIENETIENDA.COM SAS',
	'TODO EN ARTES S.A.S',
	'TODO JEANS',
	'TOTALSPORT SAS',
	'TOY PARK SAS',
	'TRASCENDENCIA HUMANA SAS',
	'TRES TRIGOS SAS',
	'TU VIVERO SAS',
	'TUNET SAS',
	'TUPPERWARE COLOMBIA SAS',
	'TUT LOGISTIC COLOMBIA SAS',
	'TYBSO S.A.S.',
	'UNIDAD DE ORIENTACION Y ASISTENCIA MATERNA',
	'UNIVERSIDAD AUTONOMA DE OCCIDENTE',
	'UNIVERSIDAD DE MANIZALES',
	'UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS',
	'UNIVERSIDAD ICESI',
	'USATI LTDA',
	'VALENTINA EUSSE',
	'VALENTINA GAVIRIA URREA',
	'VALERIA RODRIGUEZ MERCADO',
	'VARGAS Y MANTILLA SAS',
	'VARIEADES EL MUNDO DE LOS BEBES SAS',
	'VENTASOFT LTDA',
	'VERDE LIMON & CIA SAS',
	'VG ZIELCKE SAS',
	'VIAJE SIN VISA SAS',
	'VILLEGAS ASOCIADOS S.A',
	'VIÑEDO AIN KARIM',
	'VIP SERVICE GROUP',
	'VISAJU SAS',
	'VISION FOODS COLOMBIA SAS',
	'VITAL AUTOSERVICIOS SA',
	'VITAL ON LINE',
	'VIVIANA ELENA OSPINO ROJAS',
	'VIVIANA PIERNAGORDA',
	'VUELTACANELA SAS',
	'WEBEMPRESA AMERICA INC',
	'WHITMAN SAS',
	'WIDETAIL LDA',
	'WILMER ANDRES QUIÑONES HERNANDEZ',
	'WILSON HUMBERTO AGATON BELTRAN',
	'WINGS MOBILE COLOMBIA SAS',
	'WOBI COLOMBIA SAS',
	'WODEN COLOMBIA SAS',
	'WONDER GOLD SAS',
	'WORLD WILDLIFE FUND. INC WWF',
	'WOS COLOMBIA SAS',
	'WOW CAN S.A.S',
	'XPRESS ESTUDIO GRAFICO Y DIGITAL S.A',
	'YAQUI SAS',
	'YESICA ALBORNOZ',
	'YESIVIS DE LA ROSA NAVARRO',
	'YOUR NEW SELF, S.L',
	'YULIETT JOHANNA ORTIZ CONRADO',
	'ZAM LIMITADA',
	'ZAPATOS BENDECIDA',
	'ZAPATOS TEI SAS',
	'ZEPHIR SAS',
	'ZONA ECOMMERCE SAS',
	'ZORBA LACTEOS SAS',
	'ZURICH COLOMBIA SEGUROS SA',
	'	ALL NAILS BY ORGANIC SAS',
	'	LASER DERMATOLOGICO IMBANACO SA',
	'ADRENALINA BAGS Y SHOES SAS',
	'ADRIANA MENESES ESPAÑA',
	'AFE ATHLETIC FITNESS EXPERIENCE SAS',
	'ALBA LUCIA POSADA LOPEZ',
	'ALCALDIA DE VILLA DEL ROSARIO',
	'ALELI HOME DECOR SAS',
	'ALFABET SAS',
	'ALIANZA Y PROGRESO SAS',
	'ALICIA WONDERLAND',
	'ALMACENES LA 13 S.A.',
	'AM IMPORTACIONES',
	'AMARIA SOÑAR SAS',
	'ANDRES ARRUNATEGUI',
	'ANDRES FELIPE LOPEZ CABALLERO',
	'ANDRES LOPEZ GIRALDO',
	'ANDRES LOPEZ GIRALDO',
	'ARANEA SAS',
	'ARFLINA LTDA',
	'ARIS TEXTIL SAS',
	'ASADORES EL BARRIL SAS',
	'AXSPEN FASHION SAS',
	'AXSPEN FASHION SAS',
	'BEL STAR S.A.',
	'BETTER EXPERIENCE DMCC WITHOUT CVV',
	'BIKE HOUSE SAS',
	'BIKE HOUSE SAS',
	'BIKE HOUSE SAS',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BODYFIT SAS',
	'BRAYAN ALONSO REGINO TORRES',
	'BUMBLE BEE SAS',
	'C.I. COMFEMMES S.A.S',
	'C.I. GAROTAS LTDA',
	'C.I. INDUSTRIAS SUAREZ SAS',
	'CACHARRERIA CALI VARGAS & CIA. S. EN C.',
	'CAFE DE SANTA BARBARA S.A.S.',
	'CAFETALERO SAS',
	'CALZATODO S.A.',
	'CAMARA COLOMBIANA DE LA INFRAESTRUCTURA',
	'CAMARA COMERCIO DE BUCARAMANGA',
	'CAMARA DE COMERCIO DEL CAUCA',
	'CARLOS TRIANA',
	'CENTRAL COOPERATIVA DE SERVICIOS FUNERARIOS',
	'CENTRALCO LIMITADA',
	'CENTRO CULTURAL PAIDEIA',
	'CENTRO DE ABASTOS AGROPECUARIOS SAS',
	'CENTRO DE SALUD Y BELLEZA SAS',
	'CENTRO INTEGRAL DE REHABILITACION',
	'CLAUDIA MARIA VELEZ',
	'COEXITO S.A',
	'COEXITO S.A',
	'COFFEE AND ADVENTURE',
	'COLORFUL ARTE Y DISEÑO SAS',
	'COMERCIALIZADORA GRANJA PUERTA BLANCA SAS',
	'COMERCIALIZADORA LLOVIZNA SAS',
	'COMFER S.A.',
	'COMPAÑIA DE PIJAMAS GB SAS',
	'CONFEDERACION MUNDIAL DE COACHES SAS',
	'CONSEJO EPISCOPAL LATINOAMERICANO CELAM',
	'CONSTRUCTORA C Y A SAS',
	'CONSULTOR SALUD, SEMINARIO NACIONAL DE SALUD',
	'COOPERATIVA DE SERVICIOS FUNERARIOS DE SANTANDER',
	'CORPORACION PARA LA EXPRESION ARTISTICA MISI',
	'CORPORACION UNIVERSITARIA DEL CARIBE CECAR',
	'COSITAS DELICIOSAS SAS',
	'CRISALLTEX SA',
	'DAVILA ZULUAGA GROUP SAS',
	'DAVILA ZULUAGA GROUP SAS',
	'DEEP AND CO SAS',
	'DERMATOLOGICA SA',
	'DIAGNOSTIYA LIMITADA',
	'DIMA JUGUETES SAS',
	'DISTRIBUIDORA HISTRA LTDA',
	'DNSS SAS',
	'DOBLE VIA COMUNICACIONES',
	'DRA BETH SAS',
	'DUMESA S. A.',
	'DYVAL S.A. DELI THE PASTRY SHOP',
	'E-CONSULTING INTERNATIONAL S.A.S',
	'EDITORA B2B LEARNING SAS',
	'EDITORIAL BEBE GENIAL S.A.S',
	'EDITORIAL BEBE GENIAL S.A.S',
	'EDITORIAL BEBE GENIAL S.A.S',
	'EDU-CONT UNIVERSITARIA DE CATALUÑA SAS',
	'EL UNIVERSO DE LAS SORPRESAS SAS',
	'ELISA ESCOBAR PARRA',
	'EMAGISTER SERVICIOS DE FORMACION S.L.',
	'EMLAZE SYSTEMS SAS',
	'EMPRESA COOPERATIVA DE FUNERALES LOS OLIVOS LTDA.',
	'ESPAR S.A.',
	'ESPIRITUS SAS',
	'ESPUMAS PLASTICAS SA',
	'ESPUMAS PLASTICAS SA',
	'FABRICA DE CALZADO ROMULO SAS',
	'FAJAS INTERNACIONALES BY BETHEL SAS',
	'FAJAS INTERNACIONALES BY BETHEL SAS',
	'FARMACIA QUANTA S.A.S',
	'FELIPE ACEVEDO GONZALEZ',
	'FELIPE ACEVEDO GONZALEZ',
	'FESTIVALES FICE SAS',
	'FIDEICOMISOS SOCIEDAD FIDUCIARIA DE OCCIDENTE SA',
	'FIDEICOMISOS SOCIEDAD FIDUCIARIA DE OCCIDENTE SA',
	'FISIOTERAPIA EN MOVIMIENTO SAS',
	'FLORES KENNEDY SAS',
	'FOODY SAS',
	'FUNDACION FUNDANATURA',
	'FUNDACION PARA LA SEGURIDAD JURIDICA DOCUMENTAL',
	'FUNDACION SOLIDARIDAD POR COLOMBIA',
	'FUNDACIÓN ZIGMA',
	'FUNDACIÓN ZIGMA',
	'FUNDACOOEDUMAG',
	'FUNDACOOEDUMAG',
	'FUNDAGOV INTERNACIONAL SAS',
	'FUNDAGOV TELEWORK AND COWORKING SAS',
	'FUNERALES INTEGRALES SAS',
	'GLOBALITY GROUP S.A.S.',
	'GOLDEN CACTUS SAS',
	'GROUP MLS SAS',
	'GRUPO BIONICA',
	'GRUPO ILYA S.A.S.',
	'GRUPO INFESA S.A.S',
	'GRUPO IRENE MELO INTERNACIONAL SAS',
	'GRUPO MICROSISTEMAS COLOMBIA SAS',
	'GRUPO VISUAL SAS',
	'GRUPO WELCOME SA',
	'GRUPODECOR SAS',
	'GUILLERMO PULGARIN S. S.A.',
	'H&M PERMANCE ONLINE',
	'HELM FIDUCIARIA PATRIMONIO AUTONOMO- LUMNI',
	'HOME DELIGHTS SAS',
	'HOME SERVICE DE COLOMBIA LTDA',
	'IGLESIA CENTRO CRISTIANO DE ALABANZA EL SHADDAI',
	'IGLESIA CENTRO CRISTIANO DE ALABANZA EL SHADDAI',
	'IGLESIA CRISTIANA FILADELFIA JESUCRISTO VIVE',
	'IGLESIA EL MINISTERIO ROKA',
	'IKENGA S.A.S',
	'ILKO ARCOASEO SAS',
	'ILTO COLOMBIA SAS',
	'IMPOBE ALIZZ GROUP',
	'INSTITUTO SURAMERICANO SIMON BOLVIAR',
	'INTERNET ENTERPRISES HOLDING SAC',
	'INVERMUSIC G.E S.A.',
	'INVERSIONES ARRAZOLA VILLAZON Y COMPANIA LTDA',
	'INVERSIONES EDUCOLOMBIA SAS',
	'INVERSIONES ESPIRITUALES AMG SAS',
	'INVERSIONES FAJITEX S.A.S',
	'INVERSIONES FAJITEX S.A.S',
	'INVERSIONES TURISTICAS DEL CARIBE LTDA Y CIA SCA',
	'INVERSIONES Y COMERCIALIZADORA LA MILLONARIA',
	'INVERSIONES Y COMERCIALIZADORA LA MILLONARIA',
	'INVERTIMOS C.S. SAS',
	'INVERTIR MEJOR SAS',
	'INVERTIR MEJOR SAS',
	'INVESAKK',
	'IVAN ANDRES HURTADO',
	'JAMMING S.A.C',
	'JHON ALEXANDER GARRO BETANCUR',
	'JOY STAZ COMPANY S.A.S',
	'JUAN DANILO ALVERNIA PARRA',
	'JULIANA TABORDA',
	'KORADI SAS',
	'LA FARMACIA HOMEOPATICA SAS',
	'LA OPINION S.A.',
	'LA OPINION S.A.',
	'LA OPINION S.A.',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA VIE COLOMBIA SAS',
	'LANDMARK WORLDWIDE SAS',
	'LEONOR ESPINOSA DE SOSA',
	'LIGA MILITAR ECUESTRE',
	'LINA MARCELA ZAPATA MUÑOZ',
	'LIZETH DUQUE SAS',
	'LOPEZ GIRALDO ANDREA',
	'LUCKYWOMAN SAS',
	'LUIS ANGEL ROLDAN BELEÑO',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'MADERKIT S.A',
	'MADERKIT S.A',
	'MALAI S.A.S',
	'MANDARINNA BOG SAS',
	'MARIA ALEJANDRA CASTILLO RINCON',
	'MARIA CAROLINA GUTIERREZ CAMACHO',
	'MARX ALEJANDRO GUTIERREZ CUADRA',
	'MCCA GASTRONOMICA INTERNACIONAL LLC',
	'MENTA OFICIAL SAS',
	'MODASTILO SAS',
	'MONKEY BUSINESS GROUP SOCIEDAD EN ACCIONES SIMPLIFICADA',
	'MONKEY BUSINESS GROUP SOCIEDAD EN ACCIONES SIMPLIFICADA',
	'MONKEY BUSINESS GROUP SOCIEDAD EN ACCIONES SIMPLIFICADA',
	'MULTILINGUA',
	'NEEDISH - GROUPON',
	'NEEDISH - GROUPON',
	'NEEDISH - GROUPON',
	'NORMA NUÑEZ POLANCO',
	'NUT HOST S.R.L.',
	'NUVOLIS SAS',
	'NUVOLIS SAS',
	'OPERADORA MOCAWA RESORT SAS',
	'OPORTUNIDAD EMPRESARIAL SA',
	'ORQUIDEA S.A.S',
	'OTAVI S.A',
	'PALLOMARO SA',
	'PALLOMARO SA',
	'PEDRO MICHELSEN',
	'POLITECNICO INTERNACIONAL',
	'PORTO BLU SAS',
	'PREVISORA SOCIAL COOPERATIVA VIVIR',
	'PREVISORA SOCIAL COOPERATIVA VIVIR',
	'PRIVIET COLOMBIA SAS',
	'PROCESADOS E IMPORTADOS LHM SAS',
	'PROVENZAL S.A.S',
	'PROVENZAL S.A.S',
	'PYT COLOMBIA SAS',
	'RAFFAELLO NETWORK S.P.A.',
	'REVO COMMERCE SAS',
	'ROA Y MENDOZA SAS',
	'SANKI COLOMBIA S.A.S',
	'SERCOFUN LTDA',
	'SERCOFUN TULUA LTDA',
	'SERVICIOS FUNERARIOS COOPERATIVOS DE NORTE DE SANTANDER - SERFUNORTE',
	'SIGMA ELECTRONICA LTDA.',
	'SOCIEDAD COLOMBIANA DE UROLOGIA',
	'SPECIALIZED COLOMBIA S.A.S.',
	'STARGROUP CORPORATION SAS',
	'TEOMA CORP S.A.C.',
	'THE INSIDER VOX S.A.S.',
	'THERMOFORM S.A.',
	'TIENDAS DE ROPA INTIMA S.A.',
	'TIENS COLOMBIA SAS',
	'TOMAS VELEZ',
	'TOMAS VELEZ',
	'TRANQ IT EASY',
	'TRENDI TRENDS INNOVATION SAS',
	'TRENDY SHOP BTA',
	'TUTORES, ASESORIAS EMPRESARIALES S.A.S',
	'UNIVERSAL TRAVEL ASSISTANCE SAS',
	'UNIVERSAL TRAVEL ASSISTANCE SAS',
	'UNIVERSIDAD EAN',
	'UNIVERSIDAD EAN',
	'V&A FASHION LIMITADA',
	'VALENTINA ROBLEDO',
	'VICTORIA EUGENIA CASTRO TAVERA',
	'VIPS SAS',
	'VITTI SAS',
	'VIVEXCEL SAS',
	'VIVEXCEL SAS',
	'VIVEXCEL SAS',
	'WELLNESS SPA MOVIL CENTER LTDA',
	'WILSON ALFREDO MORALES ZALDUA',
	'ZONA GW SAS',
	'ZONALIBRE INGENIERIA SAS') then 'ex_vip' 
when nombre_merchant in ('FTECH COLOMBIA SAS',
	'AMERICAN SCHOOL WAY',
	'COLEGIO COLOMBIANO DE PSICOLOGOS',
	'TIENDACOL S.A.S',
	'DISTRIBUIDORA DE VINOS Y LICORES S.A.S.',
	'BOLD.CO SAS',
	'ECOTERMALES SAN VICENTE S.S',
	'LEONARDO RAMIREZ',
	'FUNDACION PARA LA EDUCACION SUPERIOR SAN MATEO',
	'UNIVERSIDAD BENITO JUAREZ ONLINE',
	'CORPORACION DE FERIAS Y EXPOSICIONES S.A.',
	'UNIVERSIDAD SERGIO ARBOLEDA',
	'SERVICIOS POSTALES NACIONALES S.A',
	'JMALUCELLI TRAVELERS SEGUROS S.A',
	'LOGISTICA FLASH COLOMBIA SAS',
	'HUBSPOT LATIN AMERICA SAS',
	'CASALIMPIA S A',
	'MOONS COLOMBIA SAS',
	'MARIA ELENA BADILLO',
	'CAJA COLOMBIANA DE SUBSIDIO FAMILIAR COLSUBSIDIO',
	'GRANADA SA',
	'UNIVERSIDAD DE LOS ANDES',
	'CIFIN S.A.S',
	'DISRUPCION AL DERECHO SAS',
	'COMERCIAL PAPELERA S.A.',
	'CONSORCIO UNIVALLE',
	'AXA ASISTENCIA COLOMBIA S.A',
	'MEDICINA LABORAL SAS',
	'MONOLEGAL S.A.S',
	'CENTRO JURIDICO INTERNACIONAL SAS',
	'CORPORACIAN MARATAN MEDELLYN',
	'ESCAPARTE SAS',
	'LOVEBRANDS SAS',
	'ESCUELA DE GASTRONOMIA GD SAS',
	'TFG LATINOAMERICA SAS',
	'CA MUEBLES Y ARQUITECTURA',
	'ENGENIS SPA',
	'EDITORIAL KUEPA SAS',
	'PRODUCTOS WOW. SAS',
	'CONEXCOL CLOUD COLOMBIA SAS',
	'MICO MEDIA GROUP SAS',
	'AUDITOOL S.A.S',
	'MANOHAY COLOMBIA S.A.S',
	'INVERSIONES Y MODA ARISTIZABAL SAS',
	'PROASISTEMAS S.A.',
	'IGNACIO SAAVEDRA',
	'INSTITUTO COLOMBO ALEMAN ICCA SPRACHINSTITUT',
	'ATM ASSISTANCE COLOMBIA',
	'NICOLAS FADUL PARDO',
	'GIROS Y FINANZAS C.F.S.A',
	'LUIS FERNANDO AVILA MANJARRES',
	'INVERSIONES TRIBEKA',
	'ONLINE INVERSIONES SAS',
	'INDIRA TATIANA GODOY POVEDA',
	'KINGS Y REBELS SAS',
	'SOCIEDAD PUERTO INDUSTRIAL AGUADULCE S.A.',
	'ESCUELA DE NEGOCIOS EUROPEA DE BARCELONA SL',
	'AVAN C LEYENDO SAS',
	'UPB',
	'AZUL & BLANCO MILLONARIOS F.C S.A',
	'LA PREVISORA S.A COMPAÑIA DE SEGUROS',
	'POPSOCKETS COLOMBIA SAS',
	'FITNESS PEOPLE',
	'BC HOTELES SA',
	'TRAFALGAR HOLDINGS DE COLOMBIA SAS',
	'CORPORACION UNIVERSITARIA IBEROAMERICANA',
	'MATEO MARULANDA CORREA',
	'CIRCULO DE VIAJES UNIVERSAL',
	'EJERCICIO INTELIGENTE SAS',
	'GRUPO MAGIA NATURAL SAS',
	'ORODHI SAS',
	'BS GRUPO COLOMBIA SAS',
	'LIGA ECUESTRE DE BOGOTA',
	'CANADIAN COLLEGE SAS',
	'PUBLICACIONES DIGITALES',
	'SESDERMA COLOMBIA S.A.',
	'DOMINA S.A',
	'ASSIST UNO ASISTENCIA AL VIAJERO SAS',
	'XUBIO, LLC',
	'SOCIEDAD COLOMBIANA DE DERECHO SAS',
	'WICCA E.U',
	'IPLER CI S.A',
	'GRUPO ALIANZA COLOMBIA SAS',
	'ESPUMAS SANTAFE DE BOGOTA SAS',
	'CORPOREOS COLOMBIA S.A.S',
	'COLOR PLUS FOTOGRAFIA SAS',
	'CORPORACION UNIVERSITARIA MINUTO DE DIOS',
	'DIGITAL INTERACTIONS SAS',
	'ENSENADA S.A',
	'COORDIUTIL S.A.',
	'AUTOLAB SAS',
	'HEEL COLOMBIA LTDA',
	'TENDENZA NOVA S A S',
	'SMART TRAINING SOCIETY SAS',
	'HERRAMIENTAS Y GESTION EDITORES CIA LTDA',
	'MANUFACTURAS REYMON SA',
	'ENLACE EDITORIAL SAS',
	'PLASTIHOGAR COLOMBIA S.A.S.',
	'LA TOTUGA',
	'CARLOS ANDRES RESTREPO CARDONA',
	'BDEAL COLOMBIA SAS',
	'SHER S.A.',
	'WOLKER',
	'FIORY',
	'SKINNY INVESTMENT SAS',
	'VD EL MUNDO A SUS PIES S.A.S',
	'FOTO DEL ORIENTE LTDA',
	'ANDREA GOMEZ',
	'INVERSIONES LCE SAS',
	'UNIVERSIDAD CUAUHTEMOC PLANTEL AGUASCALIENTES, S.C.',
	'BODEGA DE MODA S.A',
	'SAVVY CORP SAS',
	'ALIKLEAN SAS',
	'PACIFICA DE AVIACION',
	'COMPAÑIA COMERCIAL UNIVERSAL SAS',
	'SUPERTIENDAS Y DROGUERIAS OLIMPICA S.A. - OLIMPICA S.A.',
	'COMPARAONLINE COLOMBIA LTDA',
	'AYENDA SAS',
	'SEGUROPARAVIAJE.COM S.A.S',
	'FUNDACION NUEVOS HORIZONTES',
	'ZION INTERNATIONAL UNIVERSITY INC',
	'YANBAL DE COLOMBIA SA',
	'DISTRIBUIDORA MATEC SAS',
	'JULIAN OTALORA',
	'SEACRET DIRECT COLOMBIA SAS',
	'IGLESIA EL LUGAR DE SU PRESENCIA',
	'EVERNET SAS',
	'SERVICREDITO S.A',
	'DATAICO SAS',
	'CERESCOS LTDA',
	'HEALTH COMPANY INT AMERICAN MEDICAL STORE',
	'CORPORACION UNIVERSITARIA REPUBLICANA',
	'MEDPLUS MEDICINA PREPAGADA S.A.',
	'COMERCIALIZADORA TELESENTINEL LTDA',
	'WAIRUA SPA MEDICO Y DERMATOLOGIA',
	'COOINPAZ LTDA',
	'COMERCIALIZADORA PHARMASKIN SAS',
	'NATURAL ENGLISH COLOMBIA SAS',
	'FAJAS MYD POSQUIRURGICAS SAS',
	'JAMAR S.A.',
	'DISTRIBUIDORA PASTEUR S.A',
	'LATINOAMERICA HOSTING',
	'INVERSIONES MUNDO MUCURA SAS',
	'CREACIONES NADAR SA',
	'LAFAM S.A.S.',
	'LEGIS INFORMACION PROFESIONAL SA',
	'GRUPO GEARD SAS',
	'LIGA DE TENIS DE CAMPO',
	'PIXIE SAS',
	'INVERSIONES EL RAYO SAS',
	'LIBRERIA NACIONAL S.A.',
	'ECLASS COLOMBIA S.A.C.',
	'AUTOFACT COLOMBIA SAS',
	'NEBOPET SAS',
	'WORKI JOBS SAS',
	'COOPERATIVA DE AHORRO Y CREDITO DE SANTANDER LIMITADA',
	'PIANTE',
	'CAMARA DE COMERCIO DE BARRANQUILLA',
	'SOFTWARE INMOBILIARIO WASI',
	'CELLVOZ COLOMBIA SERVICIOS INTEGRALES SA ESP',
	'BRAHMA',
	'COMERCIALIZADORA DE PRODUCTOS LIFETECH SAS',
	'ROSAS DON ELOY LTDA',
	'AVCOM COLOMBIA SAS',
	'EDWCAR SAS',
	'CESAR ANDRES CASTAÑEDA MORA',
	'GROUPE SEB ANDEAN SA',
	'PREGEL COLOMBIA S.A.S.',
	'HIPERTEXTO LTDA',
	'SETCON SAS',
	'SERVIENTREGA SA',
	'JUAN CARLOS AGUILAR LOPEZ',
	'SWISSJUST LATINOAMERICA',
	'MISION CARISMATICA INTERNACIONAL',
	'EL ESPECTADOR',
	'PROSCIENCE LAB',
	'RODIL BOUTROUS & CIA LTDA',
	'SUPER REDES SAS',
	'CORPORACION UNIVERSITARIA AMERICANA',
	'CALTIAU Y GUTIERREZ SAS',
	'CORPORACION LONJA DE COLOMBIA',
	'ORTOPEDICOS FUTURO COLOMBIA',
	'SAMASA',
	'FUNDACION BANCO ARQUIDIOCESANO DE ALIMENTOS',
	'CTO MEDICINA COLOMBIA S.A.S.',
	'WELCU COLOMBIA SAS',
	'SOCIEDAD DISTRIBUIDORA DE CALZADO',
	'REDSERAUTO',
	'ASOCIACION CENTROS DE ESTUDIOS TRIBUTARIOS DE ANTIOQUIA (CETA)',
	'FUNDACION UNIVERSITARIA HORIZONTE',
	'PACIFIC INTERNATIONAL TRADE S.A.S',
	'NSDIS ANIMATION SOFTWARE S.A.',
	'ANDES BPO S.A.S',
	'IGLESIA CENTRO BIBLICO INTERNACIONAL',
	'FIT MARKET SAS',
	'CLB DEPORTIVO TEAM LA CICLERIA',
	'EXPRESO BRASILIA SA',
	'EDITORA TE LEARNING COLOMBIA SAS',
	'AGUALOGIC SAS',
	'CONSORCIO EXEQUIAL SAS',
	'3LIM2000 SAS',
	'SERVICIOS LINGUISTICOS IH COLOMBIA SAS',
	'PUBLICACIONES SEMANA',
	'LABORATORIOS EUFAR S.A.',
	'MARIANA RAMIREZ',
	'SENSEBOX SAS',
	'EDITORIAL MEDICA INTERNACIONAL LTDA',
	'SIESA PYMES S.A.S.',
	'RELISKA SAS',
	'LAURA DUPERRET',
	'INDUSTRIAS FATELARES S.A.S',
	'ENTREAGUAS',
	'SKYDROPX SAS',
	'LIGA DE TENIS DEL ATLANTICO',
	'HENRY JHAIR RUEDA RODRIGUEZ',
	'PSIGMA CORPORATION S.A.S',
	'J M C Y ASOCIADOS S.A.',
	'J&M DISTRIBUTION S.A.S.',
	'CRIYA S. A.',
	'BIO SAS',
	'EDUCACION COLOMBIA SAS',
	'CRIADERO LA CUMBRE YJ S.A.S',
	'PHONETIFY SAS',
	'INTUKANA S.A.S',
	'STUDIO 4 S.A',
	'GREEN PERFORMACE S.A.S',
	'COOPERATIVA DE TRANSPORTES VELOTAX LIMITADA',
	'PHRONESIS SAS',
	'CLICK2BUY SAS',
	'MEDPLUS CENTRO DE RECUPERACION INTEGRAL SAS',
	'SU PRESENCIA PRODUCCIONES LTDA',
	'TOTAL GP FY 23') then 'gerenciado' else 'masivo' end as gerenciado,
t2.update_
into creadas_vip_upgrade
from creadas_flag_pre_union t1
left join up_down t2 on t1.nombre_merchant = t2.name_merchant and t1.mes = t2.fecha
--limit 5
;








-----------------------------

--union union_activos_churn con creados parte 2
drop table if exists union_ac_ch_cr_2;
select *
into union_ac_ch_cr_2
from 
(select *
from union_activos_churn
union
select distinct nombre_merchant, mes, inactividad, tpt, tpv_usd, revenue_usd, gm_usd, credit_card, bank_referenced, referenced, bank_transfer, pse, debit_card, cash, cash_on_delivery, ach, lending, suma_de_metodos, inactivo, fecha_creacion_nombre, primera_trx_nombre, creacion_hasta_first_trx, mob_first_trx, mob_creacion_cuenta, created_activated, mes_anterior, back_from_churn, saldo_promedio_mes, saldo_f_mes, monto_fraude, cantidad_fraude, codigo_mcc_id, modelo_pagos, fuente_creacion, referidot, ciudad, city, plana_visa, porcentual_visa, plana_mc, porcentual_mc, plana_pse, porcentual_pse, plana_efecty, porcentual_efecty, estado_
from creadas_flag_pre_union
where flag is null) 
;








drop table if exists union_vip_upgrade;
select t1.*, case when t1.nombre_merchant in ('20/20 BIENES & SERVICIOS SAS',
	'3 GRACIAS SAS',
	'3D JUNQUERA FONT SL',
	'A MODO MIO SAS',
	'A2 HOSTING, INC.',
	'ABECEDE SAS',
	'AC COLOMBIA LAWYERS',
	'ACADEMIA DE ARTES GUERRERO',
	'ACERSHOES LTDA',
	'ACOLCHADOS EDREDONA LTDA',
	'ACSENDO S.A',
	'ACTIVE BRANDS SAS',
	'ACTUALICESE.COM LTDA',
	'ADALBERTO ANTONIO HERRERA CUELLO',
	'ADWELLCH',
	'AEROEJECUTIVOS DE ANTIOQUIA SA',
	'AG ESTUDIO SAS',
	'AGENDAPRO COLOMBIA SAS',
	'AGILEX EXPRESS S.A.S',
	'ALCANOS DE COLOMBIA S.A. ESP',
	'ALEJANDRA PATRICIA ECHAVARRIA HUTTA',
	'ALEJANDRO LOYNAZ',
	'ALEJANDRO YARCE VILLA',
	'ALFRED SAS',
	'ALIANZA BSH SAS',
	'ALIMENTOS TONING S.A',
	'ALMACENES BYBLA SAS',
	'ALMACENES CORONA S.A.S',
	'ALMACENES LA BODEGA S A',
	'ALQUIMIA DE LIBRA SAS',
	'ALTA TECNOLOGIA EN EL APRENDIZAJE S.A',
	'ALTER EGO SAS',
	'ALTIPAL',
	'ALVARO ESCANDON',
	'ALVARO LUQUE',
	'AMERICA DE CALI SA',
	'ANA ISABEL SANTA MARIA TOBON',
	'ANA MARIA GALLEGO',
	'ANA MARIA VELEZ BETANCUR',
	'ANALYTICS ACADEMY S.A.S',
	'ANCANA SAS',
	'ANDESCO',
	'ANDINOEXPRESS SL',
	'ANDREA MILANO 1932 SAS',
	'ANDREA REGGIO',
	'ANDRES FELIPE RAMIREZ RENDON',
	'ANGELA MARIA SOLARTE CRUZ',
	'ANGELICA RODRIGUEZ',
	'APASIONA T SAS',
	'APRENDER SAS',
	'AQUAROSA SAS',
	'ARQUESOFT SAS',
	'ASOCIACION COLEGIO CRISTIANO J',
	'ASOCIACION COLOMBIANA DE OTORRINOLARINGOLOGIA',
	'ASOCIACION COLOMBIANA DE PROFESORES DE FRANCES',
	'ASOCIACION DE ARTESANAS DE CHORRERA',
	'ASOCIACION NACIONAL DE INTERNOS Y RESIDENTES',
	'ASOCIACION PROFESIONAL IEEE COLOMBIA SP',
	'ASOCIACION SIN ANIMO DE LUCRO EO COLOMBIA',
	'ASSA ABLOY COLOMBIA',
	'ATC COLOMBIA SAS',
	'ATENEA ENTERPRISE SAS',
	'ATHLETIC COLOMBIA S A',
	'AUDIOCOM SAS',
	'AUREA VANESSA GARCIA',
	'AUTOELITE LTDA',
	'AUTORINES Y LLANTAS LTDA',
	'AVANTIKA COLOMBIA',
	'AVENU LEARNING LLC',
	'AYCI',
	'BALUARTE ESTUDIO JURIDICO SAS',
	'BEAUTY AND COSMETICS SAS',
	'BENDITA SEAS ROPA Y ACCESORIOS SAS',
	'BETTINA SPITZ Y CIA LTDA.',
	'BIKE GIRLS S.A.S',
	'BMC BOLIVAR SAS',
	'BMFN SAS',
	'BONITAS',
	'BONOGIFT S.A.S.',
	'BOTANIQUE S.A.S',
	'BOUTIQUE DE LA INDIA S.A.S',
	'BOY TOYS FACTORY SAS',
	'BRANDLIVE - SAMSONITE',
	'BUCCO CYCLING SAS',
	'BUENAHORA SAS',
	'BUREAU VERITAS COLOMBIA LTDA',
	'CACAO DE COLOMBIA SAS',
	'CACHARRERIA MUNDIAL',
	'CALZADOS AZALEIA',
	'CAMARA DE COMERCIO DE CALI',
	'CAMILA MONTOYA PALACIO',
	'CAMILO ARBELAEZ GOMEZ',
	'CAMINOWEB SAS',
	'CARIBE INTERCOMUNICACIONES SAS',
	'CARLOS ALBERTO TABARES GUTIERREZ',
	'CARMEN MARQUEZ',
	'CAROLINA GRILLO TARCHOPULOS',
	'CAROLINA LOZANO',
	'CAROLINA MANRIQUE TEJEDOR',
	'CARTAGENA BOAT CHARTERS SAS',
	'CARVAJAL ESPACIOS S.A.S.',
	'CASA FECHI SAS',
	'CASA SOBRE LA ROCA IGLESIA CRISTIANA INTEGRAL',
	'CASTOR MUEBLES Y ACCESORIOS SAS',
	'CATALINA MARTINEZ HERNANDEZ',
	'CATEDRAL DE SAL ZIPAQUIRA SA',
	'CAVIEDEZ Y ASOCIADOS SAS',
	'CCF CUP & CAKE FACTORY SAS',
	'CCTI SAS',
	'CENTRAL COMERCIALIZADORA',
	'CENTRO CANINO Y VETERINARIO OCHO PERROS SAS',
	'CENTRO DE DE INVESTIGACION CLINICA COLOMBIANA OBESIDAD Y METABOLISMO SAS',
	'CENTRO DE ESTUDIOS SOCIO JURIDICOS LATINOAMERICANO',
	'CENTRO DE EVENTOS ROYAL CENTER',
	'CENTRO DE EVENTOS ROYAL CENTER SAS',
	'CENTRO DE GESTION HOSPITALARIA',
	'CENTRO DE IDIOMAS ASIATICOS',
	'CENTRO PARA EL DESARROLLO INFANTIL HUELLAS SAS',
	'CENTROS DE LITERATURA CRISTIANA DE COLOMBIA C.L.C NULL',
	'CHALLENGER SAS',
	'CHEVY PLAN',
	'CHIC MARROQUINERIA LTDA',
	'CHIMENEAS DE COLOMBIA INGENIEROS ARQUITECTOS SAS',
	'CI COLOR SIETE SAS',
	'CI MIGUEL CABALLERO SAS',
	'CI SAN JOSE FARMS LTDA',
	'CIUDADDEMASCOTAS.COM SAS',
	'CLAN GADFER S.A.S',
	'CLARISSA ROSANIA SAS',
	'CLASSIC JEANS SHOP S.A.S.',
	'CLB DEPORTIVO TEAM LA CICLERIA',
	'CLERIGOS DE SAN VIATOR',
	'CLINICAS ODONTOLOGICAS DR JIMMER HERNANDEZ SAS',
	'CLOUD MEDIA SAS',
	'CLUB DEL PASEO SAS',
	'CLUB DEPORTIVO CATERPILLAR MOTOR',
	'CLUB DEPORTIVO FREDY GONZALEZ',
	'CLUB DEPORTIVO MARACANEIROS',
	'CO INTERNET',
	'COCORA INSTITUTE SAS',
	'COL HOGAR 1 SAS',
	'COLECTIVO AQUI Y AHORA SAS',
	'COLEGIO COLOMBIANO DE FISIOTERAPEUTAS COLFI',
	'COLEGIO SAN JUAN DE LOS PASTOS EU',
	'COLOMBIAN GYMWEAR',
	'COMERCIAL CARDONA HNOS. LTDA',
	'COMERCIALIZADORA BORA S.A.S',
	'COMERCIALIZADORA HERREROS SAS',
	'COMERCIALIZADORA INTERNACIONAL COCOLMEX SAS',
	'COMERCIALIZADORA INTERNACIONAL GRUPO DE MODA S A S',
	'COMFACESAR',
	'COMFATOLIMA',
	'COMPAÑIA COLOMBIANA DE CERAMICA S.A.S',
	'COMPAÑIA DE LOS MUEBLES SAS',
	'COMPAÑIA MANUFACTURERA MANISOL S.A.',
	'COMPETENTUM GROUP S.A.S.',
	'COMUNICATE FACIL COLOMBIA S.A.S.',
	'CONFECCIONES BRAVASS SAS',
	'CONFECCIONES SALOME LTDA',
	'CONGO FILMS SAS',
	'CONSEJO PROFESIONAL DE QUIMICA',
	'CONSORCIO DE CONFECCIONES S.A.',
	'CONSUMER ELECTRONIC SAS',
	'COOPERATIVA MULTIACTIVA DE LOS TRABAJADORES DE SANTANDER',
	'CORPARQUES',
	'CORPORACION AMBIENTAL COLOMBIA RESERVA DE VIDA',
	'CORPORACION AUTONOMA REGIONAL DEL VALLE DEL CAUCA',
	'COSMETIKAS SAS',
	'CREACIONES ALMAMIA SAS',
	'CRIADERO LA MARQUEZA',
	'CRISTALERIA SELMAN SAS',
	'CRISTIAN ESCOBAR MAHECHA',
	'CROYDON COLOMBIA S.A.',
	'CUBOCAMP SAS',
	'DANIEL RICARDO RODRIGUEZ CAMARGO',
	'DANIELA ARANGO',
	'DANIELA BERRIO',
	'DANIELA MARTINEZ',
	'DANIELA RUIZ',
	'DAPAC SERVICES SAS',
	'DATATTEC COLOMBIA SAS',
	'DEKO UÑAS SAS',
	'DELASIEMBRA COM SAS',
	'DENIS AMADO',
	'DEPLANTA',
	'DERMOSALUD S.A.',
	'DESINGS GROUP SAS',
	'DEVAG INGENIERIA SAS',
	'DIANA LUZ RAMIREZ CASTAÑO',
	'DIANA MARCELA CHICA SANCHEZ',
	'DIANA SERNA SERNA HERNANDEZ',
	'DIANA SOFIA POLO OSPINO',
	'DIGITAL INVESTMENT GROUP SAS',
	'DINO ANDRUS RENTERIA MONTENEGRO',
	'DISNEYLANDIA DISEÑOS S A S',
	'DISTRIBEAUTE COLOMBIA SAS',
	'DISTRIBUIDORA EL DIVINO SAS',
	'DISTRIBUNA LTDA',
	'DISTRICATOLICAS UNIDAS SAS',
	'DOMINIO AMIGO S.A.S',
	'DOMUS GLAMPING SAS',
	'DPFRATELLI SAS',
	'DULCE MENTA S.A.S.',
	'E DISTRIBUTION SAS',
	'ECOMOTORES INTERNACIONAL SAS',
	'EDITORA TKE LEARNING S.A.S.',
	'EDITORIAL ITA SAS',
	'EDITORIAL TEMIS S.A.',
	'EDUARDO JOSE CANTILLO ANGULO',
	'EDUFIRST',
	'EF SERVICIOS EN EDUCACION INTERNACIONAL SA DE CV',
	'EG RETAIL SAS',
	'EKONOMODO COLOMBIA SAS',
	'EL JARRO PRODUCTOS ALIMENTICIOS SAS',
	'ELECTROBIKE COLOMBIA SAS',
	'ELECTROFERIA DE LA CARRERA 13 SAS',
	'ELITE TRAINING LTDA',
	'ELYTE ELECTRONICA Y TELECOMUNICACIONES LTDA',
	'EMPOWER HOLDING SAS',
	'EMPRESA COLOMBIANA DE CEMENTOS S.A.S',
	'EMPRESA DE ACUEDUCTO ALCANTARILLADO Y ASEO DE MADRID ESP',
	'EN TELA LTDA',
	'ENTER SITE LTDA',
	'EQUILIBRIO SAS',
	'ERGONOMUS S.A.S',
	'ESCUELA DE NEGOCIOS CONSCIENTES S.A.S',
	'ESTEFANIA CASTRO SUAREZ',
	'ESTRATEK SAS',
	'ETNIKO SAS',
	'EUGENIA FERNANDEZ SAS',
	'EUROINNOVA FORMACION S.L.',
	'EW TECH SAS',
	'FABIANA CAMILA SANCHEZ HERNANDEZ',
	'FABY SPORT S.A.S.',
	'FAJAS PIEL DE ANGEL SAS',
	'FARMACIA DERMA LIFE',
	'FARMAPATICA COLOMBIA S.A.S',
	'FENOMENA SAS',
	'FERNEY ANTONIO PARADA CHASOY',
	'FERRIMAQ DE COLOMBIA SAS',
	'FIBER SAS',
	'FLOR ALBA CANTE AREVALO',
	'FLOR MARIA REYES PAEZ',
	'FLORISTERIA HOJAS BLANCAS S.A.S',
	'FORK CATERING AND EVENT PLANNING SAS',
	'FORUM MEDIA POLSKA SP. Z.O.O',
	'FOUR BROTHERS S.A.S.',
	'FUCSIA BOUTIQUE',
	'FUERZA GASTRONOMICA SAS',
	'FUNDACION CASATINTA COLOMBIA',
	'FUNDACION CIENTIFICA LHA',
	'FUNDACION CIGARRA',
	'FUNDACION CULTURAL ASIA IBEROAMERICA',
	'FUNDACION EL QUINTO ELEMENTO',
	'FUNDACION HOGAR NUEVA GRANADA',
	'FUNDACION INFANTIL SANTIAGO CORAZON',
	'FUNDACION JUAN FELIPE GOMEZ ESCOBAR',
	'FUNDACION MADAURA',
	'FUNDACION RED AGROECOLOGICA LA CANASTA',
	'FUNDACION UNIVERSITARIA DEL AREA ANDINA',
	'FUNERARIA Y FLORISTERIA LOS ANGELES SA',
	'GABRIEL GARZON',
	'GALAN Y CIA S. EN C. AMERICAN CHEESE CAKES',
	'GALERIA CAFE LIBRO CLUB SOCIAL PRIVADO SA',
	'GAMBOA INSTUDIO SAS',
	'GENERAL DE EQUIPOS DE COLOMBIA SA GECOLSA',
	'GERMAN ALEXANDER FRANCO BENITEZ',
	'GESCONS SAS',
	'GIGAPALMAR S.A.S.',
	'GIVELO SAS',
	'GLOBAL BLUE HYDROS',
	'GM ONLINE SAS',
	'GOLDEN WOLF TRADING SAS',
	'GOLF Y TURF SAS',
	'GRATU COLOMBIA SAS',
	'GREEN GATEWAY S.A.S',
	'GREEN GLASS COLOMBIA',
	'GRUPO ASTHEC S.A',
	'GRUPO BIROTA SAS',
	'GRUPO COLORS EQUIPOS SAS',
	'GRUPO DOX',
	'GRUPO EDITORIAL MUNDO NIÑOS',
	'GRUPO ELITE FINCA RAIZ',
	'GRUPO EMPRESARIAL AM SAS',
	'GRUPO EMPRESARIAL GEIN',
	'GRUPO EMPRESARIAL MDP',
	'GRUPO EPACHAMO SAS',
	'GRUPO FORSE SAS',
	'GRUPO HERFAL SAS',
	'GRUPO JULIO DE COLOMBIA SAS',
	'GRUPO MIS SAS',
	'GRUPO PHOENIX - MULTIDIMENSIONALES S.A.S',
	'GRUPO QUINCENA S.A.S',
	'GRUPO RENTAWEB SAS',
	'GRUPO SLAM SAS',
	'GRUPO URDA SAS',
	'GRUPO VAS A VIAJAR SAS',
	'GRUPO WONDER SA',
	'GUARNIZO Y LIZARRALDE SAS',
	'GUILLERMO PERILLA Y CIA LTDA',
	'GVS10 LTDA',
	'H3S',
	'HALCON TECNOLOGIA SAS',
	'HAPPY EUREKA SAS',
	'HCML COLOMBIA SAS',
	'HEALTHY AMERICA COLOMBIA SAS',
	'HECHO EN TURQUIA SAS',
	'HENAO PUGLIESE S A S',
	'HIPNOS BRAND SAS',
	'HOBBY CON',
	'HOJISTICA LIMITADA',
	'HOSTING RED LTDA',
	'HP COLOMBIA S.A.',
	'I LOVE GROUP SAS',
	'ICONIC STORE SAS',
	'IFRS MASTERS COLOMBIA SAS',
	'IGLESIA CRISTIANA EMANUEL - DIOS CON NOSOTROS',
	'IGLESIA MISION CARISMATICA AL MUNDO',
	'IGLESIA MISION PAZ A LAS NACIONES',
	'IMPORTADORA GORR SAS',
	'IMPORWORL SAS',
	'IN GRILL SAS',
	'IN OTHER WORDS S.A',
	'INADE SAS',
	'INDUSTRIA DE GALLETAS GRECO',
	'INDUSTRIA DE MUEBLES DEL VALLE INVAL S.A',
	'INDUSTRIAS DE ALIMENTOS CHOCONATO SAS',
	'INDUSTRIAS RAMBLER S.A.S.',
	'INDUSTRIES BERAKA EFRATA SAS',
	'INFINITY NETWORKS SAS',
	'INFLAPARQUE ACUATICO IKARUS SAS',
	'INFORMA COLOMBIA SA',
	'INGENIERIA ASISTIDA POR COMPUTADOR S.A.S',
	'INSTITUTO DE MEDICINA FUNCIONAL CJ',
	'INSUMOS Y AGREGADOS DE COLOMBIA S.A.S.',
	'INTERCLASE SRL',
	'INTERNATIONAL EXPORT BUREAU SAS',
	'INTRA MAR SHIPPING SAS',
	'INVERLEOKA S.A.S',
	'INVERLEOKA SAS',
	'INVERNATURA SAS',
	'INVERSIONES CAMPO ISLEÑO S.A',
	'INVERSIONES CORREA RUIZ LTDA',
	'INVERSIONES DERANGO SAS',
	'INVERSIONES EL CARNAL SAS',
	'INVERSIONES FLOR DE LIZ LTDA',
	'INVERSIONES MEDINA TRUJILLO',
	'INVERSIONES MERPES',
	'INVERSIONES MI TIERRITA SAS',
	'INVERSIONES MPDS SAS',
	'INVERSIONES SECURITY SAS',
	'INVERSIONES TINTIPAN SAS',
	'INVERSIONES TITI SAS',
	'IT CLOUD SERVICES',
	'JAIME',
	'JAIRO ALEXANDER CASTILLO LAMY',
	'JAVIER ALEXANDER NIETO RAMIREZ',
	'JAVIER ARMANDO ORTIZ LOPEZ',
	'JEANS MODA EXPORTACIONES',
	'JEISON VARGAS',
	'JENNY PATRICIA PLAZAS ANGEL',
	'JERUEDACIA',
	'JHON CRUZ GOMEZ',
	'JOHANNA BEATRIZ REYES GARCIA',
	'JORGE ALBERTO GUERRA CARDONA',
	'JORGE SANCHEZ',
	'JOSE A DELGADO',
	'JOSE ALEJANDRO VARGAS ANGEL',
	'JOSE URIEL TORO MANRIQUE',
	'JOSERRAGO SA',
	'JTG TORRALBA HERMANOS S EN C',
	'JUAN CASABIANCA',
	'JUAN DIEGO GOMEZ',
	'JUAN ESTEBAN CONSTAIN CROCE',
	'JUAN ESTEBAN PELAEZ GOMEZ',
	'JUAN FELIPE POSADA LONDOÑO',
	'JUAN GUILLERMO GARCES RESTREPO',
	'JUAN IGNACIO GOMEZ CORREA',
	'JUAN MIGUEL MESA RICO',
	'JUANA FRANCISCA S.A',
	'JULIAN EDUARDO ZAMORA PRIETO',
	'JULIAN GABRIEL FLOREZ ROSALES',
	'JULIANA MARIA MATIZ VEGA',
	'JULIETTE DAHIANA CASTILLO MORALES',
	'JUVENIA S.A',
	'KASA WHOLEFOODS COMPANY SAS',
	'KATHERINE ALZATE',
	'KEMISER SAS',
	'KFIR COLOMBIA S.A.S',
	'KHIRON COLOMBIA SAS',
	'KIBYS S.A.S',
	'KYDOS S.A.',
	'KYVA SAS',
	'L ATELIER DESSERT',
	'LA CARPINTERIA ARTESANAL 2016 SAS',
	'LA MAR SENSUAL SHOP SAS',
	'LABORATORIO JV URIBE M LTDA',
	'LABORATORIOS ATP SAS',
	'LABORATORIOS LEGRAND SA',
	'LABRIUT SAS',
	'LAGOBO DISTRIBUCIONES S A L G B S A',
	'LAURA ESCOBAR',
	'LAURA ROMERO',
	'LCN IDIOMAS SAS',
	'LEIDY JOHANA CRUZ',
	'LIBRERIA MEDICA CELSUS LTDA',
	'LICEO CRISTIANO VIDA NUEVA',
	'LIFETECH SAS',
	'LIGHT DE COLOMBIA S A',
	'LILIANA MORENO CASAS',
	'LILIANA PAOLA ORTIZ REYES',
	'LINK DIAZ SAS',
	'LISBEISY CAROLINA DIAZ RAMOS',
	'LIVA SOLUCION EN COMUNICACIONES',
	'LOCERIA COLOMBIANA SAS',
	'LOGISTICA Y DISTRIBUCIONES ONLINE LTDA',
	'LONJA DE COLOMBIA',
	'LORENA CUERVO DIAZ SAS',
	'LOSMILLONARIOSNET SAS',
	'LOVERA U SAS',
	'LTM3 SAS',
	'LUCAS BRAVO REYES',
	'LUIS ACONCHA',
	'LUIS DARIO BOTERO GOMEZ',
	'LUIS ENRIQUE GOMEZ DE LOS RIOS',
	'LUIS FERNANDO LOPEZ VELASQUEZ',
	'LUIS IBARDO MORALES ARIAS',
	'LUISA FERNANDA LIEVANO GARCIA',
	'LULETA SAS',
	'MAH! COLOMBIA S.A.S.',
	'MAIRA ALEJANDRA GOMEZ FONSECA',
	'MAKLIK TECHNOLOGIES SAS',
	'MANDALAS PARA EL ALMA S.A.S',
	'MANTELTEX SAS',
	'MANTIS GROUP SAS',
	'MANUFACTURAS A F SAS',
	'MANUFACTURAS KARACE SAS',
	'MAQUI SPORTSWEAR',
	'MAR BY MARISELA MONTES',
	'MAR DEL SUR LTDA',
	'MARIA ALEJANDRA PATIÑO',
	'MARIA CAMILA OCHOA NEGRETE',
	'MARIA CLAUDIA BARRIOS MENDIVIL',
	'MARIA DEL ROSARIO URIBE',
	'MARIA FERNANDA VALENCIA',
	'MARIA ISABEL JARAMILLO DIAZ',
	'MARIO ALFONSO MONTOYA PAZ',
	'MASCOTAS MAR SAS',
	'MASTER LCTL SAS',
	'MATERIALES ELECTRICOS Y MECANICOS SAS',
	'MAYRA LISSETTE GOMEZ PARRA',
	'MC DEVINS SAS',
	'MC GRAW HILL INTERAMERICANA EDITORES SA',
	'MDALATAM SAS',
	'MELOPONGO.COM.S.A.S',
	'MERCADO & PLAZA S.A.S.',
	'MERCADO COMUN SAS',
	'MERCADO VITAL SAS',
	'MERCANTE SAS',
	'MERCAVIVA COL SAS',
	'MERCEDES PIRAZA ISMARE',
	'MERIDIAN GAMING COLOMBIA',
	'MESSER COLOMBIA SA',
	'MEYPAC SAS',
	'MILENA VELASQUEZ',
	'MISIMCARDCOM',
	'MODA ACTUAL LTDA',
	'MODA OXFORD SA',
	'MODAS CLIO SAS',
	'MODERI SAS',
	'MOISES LONDONO',
	'MONASTERY COUTURE SAS',
	'MONTOC',
	'MOTOS Y ACCESORIOS SAS',
	'MOVITRONIC',
	'MUEBLES FABRICAS UNIDAS SAS',
	'MULTIAUDIO PRO LTDA',
	'MULTIMEDICO SAS',
	'MUNDIAL S.A.S',
	'MUNDIENLACE EN CONTACTO SAS',
	'MUTANTEST',
	'NAMASTE DESIGN SAS',
	'NATALIA ALTAHONA',
	'NATALIA BOTERO TORO',
	'NATALIA GONZALEZ',
	'NATURA ANAPOIMA RESERVADO EMPRESARIOS FENIX SAS',
	'NATURAL LABEL SAS',
	'NATY LONDON SAS',
	'NESS WELL S.A.S',
	'NESTOR ZULUAGA GOMEZ',
	'NETSHOP FULFILLMENT S.A.S.',
	'NICOLAS VASQUEZ',
	'NIHAO COLOMBIA SAS',
	'NINFER BETANCOURT',
	'NMV COLOMBIA S.A.S',
	'NON STOP ENTERTAINMENT SAS',
	'NUBIA CARDENAS SPA SAS',
	'NUESTRA COCINA ARTESANAL',
	'NURY CATALINA MENDIETA DIAZ',
	'NUTRABIOTICS SAS',
	'OFICOMPUTO LTDA',
	'OLFABRAND NATURAL WELLNESS SAS',
	'OLGA CRISTINA FLOREZ HERRERA',
	'OPERADORA COLOMBIANA HOTELERA SAS',
	'OPERADORA MOCAWA PLAZA SAS',
	'OPORTUNIDAD FLASH COLOMBIA SAS',
	'OPTICA ALEMANA E Y H SCHMIDT S.A.',
	'OPTIMANT COLOMBIA SAS',
	'ORGANIZACION SERIN LTDA',
	'ORTOPEDICOS WILLIAMSON Y WILLIAMSON SAS',
	'OSCAR DAVID LARA ARTURO',
	'OSCAR EDUARDO OSPINA GUERRERO',
	'OSCAR ORTEGA FLORES',
	'OUR BAG SAS',
	'PADOVA SAS',
	'PAJAROLIMON S.A.S',
	'PANAMERICANA DE DISTRIBUCIONES GARRIDO SAS',
	'PARROQUIA LA MILAGROSA',
	'PASTELERIA SALUDABLE LIBRE DE CULPA SAS',
	'PATRICIA BRICEÑO',
	'PEARSON EDUCACION DE COLOMBIA S.A.S.',
	'PEEWAH SAS',
	'PERA DK ROPA SAS',
	'PERA DK S.A.S',
	'PERCOS S. A.',
	'PHILIPPE PASTELERIA SAS',
	'PINK SECRET VIP SAS',
	'PLASTICOS ASOCIADOS S.A.',
	'PLASTIGLASS SAS',
	'PLAYA KORALIA SAS',
	'PLENIMODA SAS',
	'POLITO',
	'POLO1 SAS',
	'PONTIFICIA UNIVERSIDAD JAVERIANA',
	'PRO10',
	'PRODALIA COL SAS',
	'PRODUCTORA Y COMERCIALIZADORA DE PREDAS INTIMAS SAS CO',
	'PRODUCTOS YUPI SAS',
	'PROFY SAS',
	'PROMEDICA NATURAL BIONERGETICA SIU TUTUAVA IPS S.A',
	'PROMOFORMAS S.A.S',
	'PROMOTORA INMOBILIARIA DANN',
	'PROMOTORA PICCOLO S.A.',
	'PROTEGE TU VIAJE S.A.',
	'PROVENSAS SAS',
	'PROVIDA',
	'PUAROT COLOMBIA SAS',
	'PUNTOS Y MERCADOS SAS',
	'PURA IMAGEN LTDA',
	'QENTA SAS',
	'QUEVEDO TORRES LTDA',
	'RAFAEL FRANCISCO ZUÑIGA',
	'RAFAEL MAURICIO NUÑEZ GARZON',
	'RAMIRO TORO GUARIN',
	'RAPIMERCAR LTDA',
	'RAYJAR SAS',
	'RCA & ASOCIADOS SAS',
	'REA SOLUCIONES SAS',
	'RED DE PEDAGOGAA SAS',
	'REDES ORION SAS',
	'REDFRED SAS',
	'REGION SIMPLIFICADO',
	'REIZEN SAS',
	'REMG INGENIERIA SAS',
	'RENA WARE DE COLOMBIA S.A',
	'REPRODUCCION ANIMAL BIOTECNOLOGICA',
	'RESEM COLOMBIA S.A.S',
	'RESERVA ONE LOVE SAS',
	'RESTAURANTE MUY SAS',
	'RETRO KNOB S.A.S.',
	'RFG REPRESENTACIONES SAS',
	'RFID TECNOLOGIA SAS',
	'RICARDO ANTONIO ORTEGA VILLEGAS',
	'RICARDO FRAILE ROJAS',
	'RICARDO RAMIREZ',
	'RICARDO SANTANA',
	'RICHARD ALEXANDER LUGO PIRAQUIVE',
	'RISKS INTERNATIONAL SAS',
	'RODRIGUEZ IGUARAN SAS',
	'ROGGER ADRIAN CARDONA LOPEZ',
	'ROJAS TRASTEOS SERVICIOS SA',
	'RONDA S.A',
	'ROYAL ELIM INTERNACIONAL SAS',
	'ROYAL SAS',
	'SALUD SEMILLAS PLATAFORME',
	'SANNUS FOODS SAS',
	'SANTA COSTILLA SAS',
	'SANTANA SAS',
	'SANTIAGO ANDRES MENDIETA PEREZ',
	'SANTIAGO ANDRES OSORIO ARBOLEDA',
	'SANTIAGO BOTERO',
	'SARA ECHEVERRI',
	'SARA FERNANDEZ GOMEZ',
	'SARA MARIA TOBO YEPES',
	'SARA RUA SIERRA',
	'SARAI CLOTHING S.A',
	'SATLOCK LOGISTICA Y SEGURIDAD SAS',
	'SATRACK INC DE COLOMBIA SERVISAT SAS',
	'SAVA OUTSORCING SAS ZOMAC',
	'SCHALLER DESIGN AND TECHNOLOGY SAS',
	'SCHWARTZ BUSINESS SOLUTIONS S.A.S',
	'SCOTCHLAND SAS',
	'SEBASTIAN CARDONA GIRALDO',
	'SEBASTIAN MONSALVE CORREA',
	'SELETTI SAS',
	'SEOUL MEDICINA ESTETICA INTEGRAL & SPA',
	'SERGIO Y ALEXANDRA RADA SAS',
	'SERPRO DIGITAL SAS',
	'SESAMOTEX SAS',
	'SETROC MOBILE GROUP SAS',
	'SHERYL SAIZ',
	'SHOEMASTERS S.A.S',
	'SI SAS',
	'SICK UNIFORMS',
	'SIEMBRAVIVA SAS',
	'SIESUA MEDICINA LASER Y SPA SAS',
	'SILVIA ALEJANDRA NUÑEZ GARZON',
	'SILVIA JULIANA ORTIZ',
	'SILVIA PAOLA NAVARRETE VENEGAS',
	'SIMONIZ SA',
	'SISTEMAS INTELIGENTES Y TECNOLOGIA S',
	'SISTEMAS MODULARES DE ALUMINIO SAS',
	'SKENA SAS',
	'SMILEFUL COLOMBIA S.A.S.',
	'SOBERANA SAS',
	'SOCIEDAD COMERCIAL ZAM LIMITADA',
	'SOCIEDAD PARA EL AVANCE DE LA PSICOTERAPIA CENTRAD',
	'SOCIEDAD PORTUARIA REGIONAL DE BUENAVENTURA S.A.',
	'SOCODA SAS',
	'SPORTCHECK SAS',
	'STARWEAR INTERNATIONAL S.A',
	'STIT SKINCARE AND BEAUTY SAS',
	'SU FABRICA DE VENTAS SAS',
	'SUEÑA Y CREA INVERSIONES SAS',
	'SUFIES EDUCACION SAS',
	'SUMINISTROS DACAR SAS',
	'SUMINISTROS DE COLOMBIA S.A.S.',
	'SUMMERHOUSE SAS',
	'SUPER DE',
	'SUPERMERCADO NATURISTA LTDA',
	'SUPERSIGNS SAS',
	'SURFERS INTERACTIVE SAS',
	'SUSANA MEJIA GAVIRIA',
	'SVELTHUS CLAIICA DE REJUVENECIMIENTO FACIAL Y COR',
	'SYB COLOMBIA S.A.S',
	'TABA SPORT S.A.S.',
	'TALLER ALVAREZ VILLA SAS',
	'TE DELUXE GROUP S.A.S',
	'TEAM FLOWERS COLOMBIA',
	'TECNIFACIL SAS',
	'TECNOPTIX SAS',
	'TELEACCION SAS',
	'TERRAMAGA SAS',
	'TEXTILES SWANTEX S A',
	'TEXTILES VELANEX S.A',
	'TEXTILES VMG LTDA',
	'TEXTRON',
	'THE DREAMER OPERATIONS SAS',
	'THUNDERBOLT SAS',
	'TIENETIENDA.COM SAS',
	'TODO EN ARTES S.A.S',
	'TODO JEANS',
	'TOTALSPORT SAS',
	'TOY PARK SAS',
	'TRASCENDENCIA HUMANA SAS',
	'TRES TRIGOS SAS',
	'TU VIVERO SAS',
	'TUNET SAS',
	'TUPPERWARE COLOMBIA SAS',
	'TUT LOGISTIC COLOMBIA SAS',
	'TYBSO S.A.S.',
	'UNIDAD DE ORIENTACION Y ASISTENCIA MATERNA',
	'UNIVERSIDAD AUTONOMA DE OCCIDENTE',
	'UNIVERSIDAD DE MANIZALES',
	'UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS',
	'UNIVERSIDAD ICESI',
	'USATI LTDA',
	'VALENTINA EUSSE',
	'VALENTINA GAVIRIA URREA',
	'VALERIA RODRIGUEZ MERCADO',
	'VARGAS Y MANTILLA SAS',
	'VARIEADES EL MUNDO DE LOS BEBES SAS',
	'VENTASOFT LTDA',
	'VERDE LIMON & CIA SAS',
	'VG ZIELCKE SAS',
	'VIAJE SIN VISA SAS',
	'VILLEGAS ASOCIADOS S.A',
	'VIÑEDO AIN KARIM',
	'VIP SERVICE GROUP',
	'VISAJU SAS',
	'VISION FOODS COLOMBIA SAS',
	'VITAL AUTOSERVICIOS SA',
	'VITAL ON LINE',
	'VIVIANA ELENA OSPINO ROJAS',
	'VIVIANA PIERNAGORDA',
	'VUELTACANELA SAS',
	'WEBEMPRESA AMERICA INC',
	'WHITMAN SAS',
	'WIDETAIL LDA',
	'WILMER ANDRES QUIÑONES HERNANDEZ',
	'WILSON HUMBERTO AGATON BELTRAN',
	'WINGS MOBILE COLOMBIA SAS',
	'WOBI COLOMBIA SAS',
	'WODEN COLOMBIA SAS',
	'WONDER GOLD SAS',
	'WORLD WILDLIFE FUND. INC WWF',
	'WOS COLOMBIA SAS',
	'WOW CAN S.A.S',
	'XPRESS ESTUDIO GRAFICO Y DIGITAL S.A',
	'YAQUI SAS',
	'YESICA ALBORNOZ',
	'YESIVIS DE LA ROSA NAVARRO',
	'YOUR NEW SELF, S.L',
	'YULIETT JOHANNA ORTIZ CONRADO',
	'ZAM LIMITADA',
	'ZAPATOS BENDECIDA',
	'ZAPATOS TEI SAS',
	'ZEPHIR SAS',
	'ZONA ECOMMERCE SAS',
	'ZORBA LACTEOS SAS',
	'ZURICH COLOMBIA SEGUROS SA',
	'	ALL NAILS BY ORGANIC SAS',
	'	LASER DERMATOLOGICO IMBANACO SA',
	'ADRENALINA BAGS Y SHOES SAS',
	'ADRIANA MENESES ESPAÑA',
	'AFE ATHLETIC FITNESS EXPERIENCE SAS',
	'ALBA LUCIA POSADA LOPEZ',
	'ALCALDIA DE VILLA DEL ROSARIO',
	'ALELI HOME DECOR SAS',
	'ALFABET SAS',
	'ALIANZA Y PROGRESO SAS',
	'ALICIA WONDERLAND',
	'ALMACENES LA 13 S.A.',
	'AM IMPORTACIONES',
	'AMARIA SOÑAR SAS',
	'ANDRES ARRUNATEGUI',
	'ANDRES FELIPE LOPEZ CABALLERO',
	'ANDRES LOPEZ GIRALDO',
	'ANDRES LOPEZ GIRALDO',
	'ARANEA SAS',
	'ARFLINA LTDA',
	'ARIS TEXTIL SAS',
	'ASADORES EL BARRIL SAS',
	'AXSPEN FASHION SAS',
	'AXSPEN FASHION SAS',
	'BEL STAR S.A.',
	'BETTER EXPERIENCE DMCC WITHOUT CVV',
	'BIKE HOUSE SAS',
	'BIKE HOUSE SAS',
	'BIKE HOUSE SAS',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BLUEFIELDS FINANCIAL COLOMBIA',
	'BODYFIT SAS',
	'BRAYAN ALONSO REGINO TORRES',
	'BUMBLE BEE SAS',
	'C.I. COMFEMMES S.A.S',
	'C.I. GAROTAS LTDA',
	'C.I. INDUSTRIAS SUAREZ SAS',
	'CACHARRERIA CALI VARGAS & CIA. S. EN C.',
	'CAFE DE SANTA BARBARA S.A.S.',
	'CAFETALERO SAS',
	'CALZATODO S.A.',
	'CAMARA COLOMBIANA DE LA INFRAESTRUCTURA',
	'CAMARA COMERCIO DE BUCARAMANGA',
	'CAMARA DE COMERCIO DEL CAUCA',
	'CARLOS TRIANA',
	'CENTRAL COOPERATIVA DE SERVICIOS FUNERARIOS',
	'CENTRALCO LIMITADA',
	'CENTRO CULTURAL PAIDEIA',
	'CENTRO DE ABASTOS AGROPECUARIOS SAS',
	'CENTRO DE SALUD Y BELLEZA SAS',
	'CENTRO INTEGRAL DE REHABILITACION',
	'CLAUDIA MARIA VELEZ',
	'COEXITO S.A',
	'COEXITO S.A',
	'COFFEE AND ADVENTURE',
	'COLORFUL ARTE Y DISEÑO SAS',
	'COMERCIALIZADORA GRANJA PUERTA BLANCA SAS',
	'COMERCIALIZADORA LLOVIZNA SAS',
	'COMFER S.A.',
	'COMPAÑIA DE PIJAMAS GB SAS',
	'CONFEDERACION MUNDIAL DE COACHES SAS',
	'CONSEJO EPISCOPAL LATINOAMERICANO CELAM',
	'CONSTRUCTORA C Y A SAS',
	'CONSULTOR SALUD, SEMINARIO NACIONAL DE SALUD',
	'COOPERATIVA DE SERVICIOS FUNERARIOS DE SANTANDER',
	'CORPORACION PARA LA EXPRESION ARTISTICA MISI',
	'CORPORACION UNIVERSITARIA DEL CARIBE CECAR',
	'COSITAS DELICIOSAS SAS',
	'CRISALLTEX SA',
	'DAVILA ZULUAGA GROUP SAS',
	'DAVILA ZULUAGA GROUP SAS',
	'DEEP AND CO SAS',
	'DERMATOLOGICA SA',
	'DIAGNOSTIYA LIMITADA',
	'DIMA JUGUETES SAS',
	'DISTRIBUIDORA HISTRA LTDA',
	'DNSS SAS',
	'DOBLE VIA COMUNICACIONES',
	'DRA BETH SAS',
	'DUMESA S. A.',
	'DYVAL S.A. DELI THE PASTRY SHOP',
	'E-CONSULTING INTERNATIONAL S.A.S',
	'EDITORA B2B LEARNING SAS',
	'EDITORIAL BEBE GENIAL S.A.S',
	'EDITORIAL BEBE GENIAL S.A.S',
	'EDITORIAL BEBE GENIAL S.A.S',
	'EDU-CONT UNIVERSITARIA DE CATALUÑA SAS',
	'EL UNIVERSO DE LAS SORPRESAS SAS',
	'ELISA ESCOBAR PARRA',
	'EMAGISTER SERVICIOS DE FORMACION S.L.',
	'EMLAZE SYSTEMS SAS',
	'EMPRESA COOPERATIVA DE FUNERALES LOS OLIVOS LTDA.',
	'ESPAR S.A.',
	'ESPIRITUS SAS',
	'ESPUMAS PLASTICAS SA',
	'ESPUMAS PLASTICAS SA',
	'FABRICA DE CALZADO ROMULO SAS',
	'FAJAS INTERNACIONALES BY BETHEL SAS',
	'FAJAS INTERNACIONALES BY BETHEL SAS',
	'FARMACIA QUANTA S.A.S',
	'FELIPE ACEVEDO GONZALEZ',
	'FELIPE ACEVEDO GONZALEZ',
	'FESTIVALES FICE SAS',
	'FIDEICOMISOS SOCIEDAD FIDUCIARIA DE OCCIDENTE SA',
	'FIDEICOMISOS SOCIEDAD FIDUCIARIA DE OCCIDENTE SA',
	'FISIOTERAPIA EN MOVIMIENTO SAS',
	'FLORES KENNEDY SAS',
	'FOODY SAS',
	'FUNDACION FUNDANATURA',
	'FUNDACION PARA LA SEGURIDAD JURIDICA DOCUMENTAL',
	'FUNDACION SOLIDARIDAD POR COLOMBIA',
	'FUNDACIÓN ZIGMA',
	'FUNDACIÓN ZIGMA',
	'FUNDACOOEDUMAG',
	'FUNDACOOEDUMAG',
	'FUNDAGOV INTERNACIONAL SAS',
	'FUNDAGOV TELEWORK AND COWORKING SAS',
	'FUNERALES INTEGRALES SAS',
	'GLOBALITY GROUP S.A.S.',
	'GOLDEN CACTUS SAS',
	'GROUP MLS SAS',
	'GRUPO BIONICA',
	'GRUPO ILYA S.A.S.',
	'GRUPO INFESA S.A.S',
	'GRUPO IRENE MELO INTERNACIONAL SAS',
	'GRUPO MICROSISTEMAS COLOMBIA SAS',
	'GRUPO VISUAL SAS',
	'GRUPO WELCOME SA',
	'GRUPODECOR SAS',
	'GUILLERMO PULGARIN S. S.A.',
	'H&M PERMANCE ONLINE',
	'HELM FIDUCIARIA PATRIMONIO AUTONOMO- LUMNI',
	'HOME DELIGHTS SAS',
	'HOME SERVICE DE COLOMBIA LTDA',
	'IGLESIA CENTRO CRISTIANO DE ALABANZA EL SHADDAI',
	'IGLESIA CENTRO CRISTIANO DE ALABANZA EL SHADDAI',
	'IGLESIA CRISTIANA FILADELFIA JESUCRISTO VIVE',
	'IGLESIA EL MINISTERIO ROKA',
	'IKENGA S.A.S',
	'ILKO ARCOASEO SAS',
	'ILTO COLOMBIA SAS',
	'IMPOBE ALIZZ GROUP',
	'INSTITUTO SURAMERICANO SIMON BOLVIAR',
	'INTERNET ENTERPRISES HOLDING SAC',
	'INVERMUSIC G.E S.A.',
	'INVERSIONES ARRAZOLA VILLAZON Y COMPANIA LTDA',
	'INVERSIONES EDUCOLOMBIA SAS',
	'INVERSIONES ESPIRITUALES AMG SAS',
	'INVERSIONES FAJITEX S.A.S',
	'INVERSIONES FAJITEX S.A.S',
	'INVERSIONES TURISTICAS DEL CARIBE LTDA Y CIA SCA',
	'INVERSIONES Y COMERCIALIZADORA LA MILLONARIA',
	'INVERSIONES Y COMERCIALIZADORA LA MILLONARIA',
	'INVERTIMOS C.S. SAS',
	'INVERTIR MEJOR SAS',
	'INVERTIR MEJOR SAS',
	'INVESAKK',
	'IVAN ANDRES HURTADO',
	'JAMMING S.A.C',
	'JHON ALEXANDER GARRO BETANCUR',
	'JOY STAZ COMPANY S.A.S',
	'JUAN DANILO ALVERNIA PARRA',
	'JULIANA TABORDA',
	'KORADI SAS',
	'LA FARMACIA HOMEOPATICA SAS',
	'LA OPINION S.A.',
	'LA OPINION S.A.',
	'LA OPINION S.A.',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA RUTA COLOMBIA SAS',
	'LA VIE COLOMBIA SAS',
	'LANDMARK WORLDWIDE SAS',
	'LEONOR ESPINOSA DE SOSA',
	'LIGA MILITAR ECUESTRE',
	'LINA MARCELA ZAPATA MUÑOZ',
	'LIZETH DUQUE SAS',
	'LOPEZ GIRALDO ANDREA',
	'LUCKYWOMAN SAS',
	'LUIS ANGEL ROLDAN BELEÑO',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'LUMNI COLOMBIA SAS',
	'MADERKIT S.A',
	'MADERKIT S.A',
	'MALAI S.A.S',
	'MANDARINNA BOG SAS',
	'MARIA ALEJANDRA CASTILLO RINCON',
	'MARIA CAROLINA GUTIERREZ CAMACHO',
	'MARX ALEJANDRO GUTIERREZ CUADRA',
	'MCCA GASTRONOMICA INTERNACIONAL LLC',
	'MENTA OFICIAL SAS',
	'MODASTILO SAS',
	'MONKEY BUSINESS GROUP SOCIEDAD EN ACCIONES SIMPLIFICADA',
	'MONKEY BUSINESS GROUP SOCIEDAD EN ACCIONES SIMPLIFICADA',
	'MONKEY BUSINESS GROUP SOCIEDAD EN ACCIONES SIMPLIFICADA',
	'MULTILINGUA',
	'NEEDISH - GROUPON',
	'NEEDISH - GROUPON',
	'NEEDISH - GROUPON',
	'NORMA NUÑEZ POLANCO',
	'NUT HOST S.R.L.',
	'NUVOLIS SAS',
	'NUVOLIS SAS',
	'OPERADORA MOCAWA RESORT SAS',
	'OPORTUNIDAD EMPRESARIAL SA',
	'ORQUIDEA S.A.S',
	'OTAVI S.A',
	'PALLOMARO SA',
	'PALLOMARO SA',
	'PEDRO MICHELSEN',
	'POLITECNICO INTERNACIONAL',
	'PORTO BLU SAS',
	'PREVISORA SOCIAL COOPERATIVA VIVIR',
	'PREVISORA SOCIAL COOPERATIVA VIVIR',
	'PRIVIET COLOMBIA SAS',
	'PROCESADOS E IMPORTADOS LHM SAS',
	'PROVENZAL S.A.S',
	'PROVENZAL S.A.S',
	'PYT COLOMBIA SAS',
	'RAFFAELLO NETWORK S.P.A.',
	'REVO COMMERCE SAS',
	'ROA Y MENDOZA SAS',
	'SANKI COLOMBIA S.A.S',
	'SERCOFUN LTDA',
	'SERCOFUN TULUA LTDA',
	'SERVICIOS FUNERARIOS COOPERATIVOS DE NORTE DE SANTANDER - SERFUNORTE',
	'SIGMA ELECTRONICA LTDA.',
	'SOCIEDAD COLOMBIANA DE UROLOGIA',
	'SPECIALIZED COLOMBIA S.A.S.',
	'STARGROUP CORPORATION SAS',
	'TEOMA CORP S.A.C.',
	'THE INSIDER VOX S.A.S.',
	'THERMOFORM S.A.',
	'TIENDAS DE ROPA INTIMA S.A.',
	'TIENS COLOMBIA SAS',
	'TOMAS VELEZ',
	'TOMAS VELEZ',
	'TRANQ IT EASY',
	'TRENDI TRENDS INNOVATION SAS',
	'TRENDY SHOP BTA',
	'TUTORES, ASESORIAS EMPRESARIALES S.A.S',
	'UNIVERSAL TRAVEL ASSISTANCE SAS',
	'UNIVERSAL TRAVEL ASSISTANCE SAS',
	'UNIVERSIDAD EAN',
	'UNIVERSIDAD EAN',
	'V&A FASHION LIMITADA',
	'VALENTINA ROBLEDO',
	'VICTORIA EUGENIA CASTRO TAVERA',
	'VIPS SAS',
	'VITTI SAS',
	'VIVEXCEL SAS',
	'VIVEXCEL SAS',
	'VIVEXCEL SAS',
	'WELLNESS SPA MOVIL CENTER LTDA',
	'WILSON ALFREDO MORALES ZALDUA',
	'ZONA GW SAS',
	'ZONALIBRE INGENIERIA SAS') then 'ex_vip' 
when nombre_merchant in ('FTECH COLOMBIA SAS',
	'AMERICAN SCHOOL WAY',
	'COLEGIO COLOMBIANO DE PSICOLOGOS',
	'TIENDACOL S.A.S',
	'DISTRIBUIDORA DE VINOS Y LICORES S.A.S.',
	'BOLD.CO SAS',
	'ECOTERMALES SAN VICENTE S.S',
	'LEONARDO RAMIREZ',
	'FUNDACION PARA LA EDUCACION SUPERIOR SAN MATEO',
	'UNIVERSIDAD BENITO JUAREZ ONLINE',
	'CORPORACION DE FERIAS Y EXPOSICIONES S.A.',
	'UNIVERSIDAD SERGIO ARBOLEDA',
	'SERVICIOS POSTALES NACIONALES S.A',
	'JMALUCELLI TRAVELERS SEGUROS S.A',
	'LOGISTICA FLASH COLOMBIA SAS',
	'HUBSPOT LATIN AMERICA SAS',
	'CASALIMPIA S A',
	'MOONS COLOMBIA SAS',
	'MARIA ELENA BADILLO',
	'CAJA COLOMBIANA DE SUBSIDIO FAMILIAR COLSUBSIDIO',
	'GRANADA SA',
	'UNIVERSIDAD DE LOS ANDES',
	'CIFIN S.A.S',
	'DISRUPCION AL DERECHO SAS',
	'COMERCIAL PAPELERA S.A.',
	'CONSORCIO UNIVALLE',
	'AXA ASISTENCIA COLOMBIA S.A',
	'MEDICINA LABORAL SAS',
	'MONOLEGAL S.A.S',
	'CENTRO JURIDICO INTERNACIONAL SAS',
	'CORPORACIAN MARATAN MEDELLYN',
	'ESCAPARTE SAS',
	'LOVEBRANDS SAS',
	'ESCUELA DE GASTRONOMIA GD SAS',
	'TFG LATINOAMERICA SAS',
	'CA MUEBLES Y ARQUITECTURA',
	'ENGENIS SPA',
	'EDITORIAL KUEPA SAS',
	'PRODUCTOS WOW. SAS',
	'CONEXCOL CLOUD COLOMBIA SAS',
	'MICO MEDIA GROUP SAS',
	'AUDITOOL S.A.S',
	'MANOHAY COLOMBIA S.A.S',
	'INVERSIONES Y MODA ARISTIZABAL SAS',
	'PROASISTEMAS S.A.',
	'IGNACIO SAAVEDRA',
	'INSTITUTO COLOMBO ALEMAN ICCA SPRACHINSTITUT',
	'ATM ASSISTANCE COLOMBIA',
	'NICOLAS FADUL PARDO',
	'GIROS Y FINANZAS C.F.S.A',
	'LUIS FERNANDO AVILA MANJARRES',
	'INVERSIONES TRIBEKA',
	'ONLINE INVERSIONES SAS',
	'INDIRA TATIANA GODOY POVEDA',
	'KINGS Y REBELS SAS',
	'SOCIEDAD PUERTO INDUSTRIAL AGUADULCE S.A.',
	'ESCUELA DE NEGOCIOS EUROPEA DE BARCELONA SL',
	'AVAN C LEYENDO SAS',
	'UPB',
	'AZUL & BLANCO MILLONARIOS F.C S.A',
	'LA PREVISORA S.A COMPAÑIA DE SEGUROS',
	'POPSOCKETS COLOMBIA SAS',
	'FITNESS PEOPLE',
	'BC HOTELES SA',
	'TRAFALGAR HOLDINGS DE COLOMBIA SAS',
	'CORPORACION UNIVERSITARIA IBEROAMERICANA',
	'MATEO MARULANDA CORREA',
	'CIRCULO DE VIAJES UNIVERSAL',
	'EJERCICIO INTELIGENTE SAS',
	'GRUPO MAGIA NATURAL SAS',
	'ORODHI SAS',
	'BS GRUPO COLOMBIA SAS',
	'LIGA ECUESTRE DE BOGOTA',
	'CANADIAN COLLEGE SAS',
	'PUBLICACIONES DIGITALES',
	'SESDERMA COLOMBIA S.A.',
	'DOMINA S.A',
	'ASSIST UNO ASISTENCIA AL VIAJERO SAS',
	'XUBIO, LLC',
	'SOCIEDAD COLOMBIANA DE DERECHO SAS',
	'WICCA E.U',
	'IPLER CI S.A',
	'GRUPO ALIANZA COLOMBIA SAS',
	'ESPUMAS SANTAFE DE BOGOTA SAS',
	'CORPOREOS COLOMBIA S.A.S',
	'COLOR PLUS FOTOGRAFIA SAS',
	'CORPORACION UNIVERSITARIA MINUTO DE DIOS',
	'DIGITAL INTERACTIONS SAS',
	'ENSENADA S.A',
	'COORDIUTIL S.A.',
	'AUTOLAB SAS',
	'HEEL COLOMBIA LTDA',
	'TENDENZA NOVA S A S',
	'SMART TRAINING SOCIETY SAS',
	'HERRAMIENTAS Y GESTION EDITORES CIA LTDA',
	'MANUFACTURAS REYMON SA',
	'ENLACE EDITORIAL SAS',
	'PLASTIHOGAR COLOMBIA S.A.S.',
	'LA TOTUGA',
	'CARLOS ANDRES RESTREPO CARDONA',
	'BDEAL COLOMBIA SAS',
	'SHER S.A.',
	'WOLKER',
	'FIORY',
	'SKINNY INVESTMENT SAS',
	'VD EL MUNDO A SUS PIES S.A.S',
	'FOTO DEL ORIENTE LTDA',
	'ANDREA GOMEZ',
	'INVERSIONES LCE SAS',
	'UNIVERSIDAD CUAUHTEMOC PLANTEL AGUASCALIENTES, S.C.',
	'BODEGA DE MODA S.A',
	'SAVVY CORP SAS',
	'ALIKLEAN SAS',
	'PACIFICA DE AVIACION',
	'COMPAÑIA COMERCIAL UNIVERSAL SAS',
	'SUPERTIENDAS Y DROGUERIAS OLIMPICA S.A. - OLIMPICA S.A.',
	'COMPARAONLINE COLOMBIA LTDA',
	'AYENDA SAS',
	'SEGUROPARAVIAJE.COM S.A.S',
	'FUNDACION NUEVOS HORIZONTES',
	'ZION INTERNATIONAL UNIVERSITY INC',
	'YANBAL DE COLOMBIA SA',
	'DISTRIBUIDORA MATEC SAS',
	'JULIAN OTALORA',
	'SEACRET DIRECT COLOMBIA SAS',
	'IGLESIA EL LUGAR DE SU PRESENCIA',
	'EVERNET SAS',
	'SERVICREDITO S.A',
	'DATAICO SAS',
	'CERESCOS LTDA',
	'HEALTH COMPANY INT AMERICAN MEDICAL STORE',
	'CORPORACION UNIVERSITARIA REPUBLICANA',
	'MEDPLUS MEDICINA PREPAGADA S.A.',
	'COMERCIALIZADORA TELESENTINEL LTDA',
	'WAIRUA SPA MEDICO Y DERMATOLOGIA',
	'COOINPAZ LTDA',
	'COMERCIALIZADORA PHARMASKIN SAS',
	'NATURAL ENGLISH COLOMBIA SAS',
	'FAJAS MYD POSQUIRURGICAS SAS',
	'JAMAR S.A.',
	'DISTRIBUIDORA PASTEUR S.A',
	'LATINOAMERICA HOSTING',
	'INVERSIONES MUNDO MUCURA SAS',
	'CREACIONES NADAR SA',
	'LAFAM S.A.S.',
	'LEGIS INFORMACION PROFESIONAL SA',
	'GRUPO GEARD SAS',
	'LIGA DE TENIS DE CAMPO',
	'PIXIE SAS',
	'INVERSIONES EL RAYO SAS',
	'LIBRERIA NACIONAL S.A.',
	'ECLASS COLOMBIA S.A.C.',
	'AUTOFACT COLOMBIA SAS',
	'NEBOPET SAS',
	'WORKI JOBS SAS',
	'COOPERATIVA DE AHORRO Y CREDITO DE SANTANDER LIMITADA',
	'PIANTE',
	'CAMARA DE COMERCIO DE BARRANQUILLA',
	'SOFTWARE INMOBILIARIO WASI',
	'CELLVOZ COLOMBIA SERVICIOS INTEGRALES SA ESP',
	'BRAHMA',
	'COMERCIALIZADORA DE PRODUCTOS LIFETECH SAS',
	'ROSAS DON ELOY LTDA',
	'AVCOM COLOMBIA SAS',
	'EDWCAR SAS',
	'CESAR ANDRES CASTAÑEDA MORA',
	'GROUPE SEB ANDEAN SA',
	'PREGEL COLOMBIA S.A.S.',
	'HIPERTEXTO LTDA',
	'SETCON SAS',
	'SERVIENTREGA SA',
	'JUAN CARLOS AGUILAR LOPEZ',
	'SWISSJUST LATINOAMERICA',
	'MISION CARISMATICA INTERNACIONAL',
	'EL ESPECTADOR',
	'PROSCIENCE LAB',
	'RODIL BOUTROUS & CIA LTDA',
	'SUPER REDES SAS',
	'CORPORACION UNIVERSITARIA AMERICANA',
	'CALTIAU Y GUTIERREZ SAS',
	'CORPORACION LONJA DE COLOMBIA',
	'ORTOPEDICOS FUTURO COLOMBIA',
	'SAMASA',
	'FUNDACION BANCO ARQUIDIOCESANO DE ALIMENTOS',
	'CTO MEDICINA COLOMBIA S.A.S.',
	'WELCU COLOMBIA SAS',
	'SOCIEDAD DISTRIBUIDORA DE CALZADO',
	'REDSERAUTO',
	'ASOCIACION CENTROS DE ESTUDIOS TRIBUTARIOS DE ANTIOQUIA (CETA)',
	'FUNDACION UNIVERSITARIA HORIZONTE',
	'PACIFIC INTERNATIONAL TRADE S.A.S',
	'NSDIS ANIMATION SOFTWARE S.A.',
	'ANDES BPO S.A.S',
	'IGLESIA CENTRO BIBLICO INTERNACIONAL',
	'FIT MARKET SAS',
	'CLB DEPORTIVO TEAM LA CICLERIA',
	'EXPRESO BRASILIA SA',
	'EDITORA TE LEARNING COLOMBIA SAS',
	'AGUALOGIC SAS',
	'CONSORCIO EXEQUIAL SAS',
	'3LIM2000 SAS',
	'SERVICIOS LINGUISTICOS IH COLOMBIA SAS',
	'PUBLICACIONES SEMANA',
	'LABORATORIOS EUFAR S.A.',
	'MARIANA RAMIREZ',
	'SENSEBOX SAS',
	'EDITORIAL MEDICA INTERNACIONAL LTDA',
	'SIESA PYMES S.A.S.',
	'RELISKA SAS',
	'LAURA DUPERRET',
	'INDUSTRIAS FATELARES S.A.S',
	'ENTREAGUAS',
	'SKYDROPX SAS',
	'LIGA DE TENIS DEL ATLANTICO',
	'HENRY JHAIR RUEDA RODRIGUEZ',
	'PSIGMA CORPORATION S.A.S',
	'J M C Y ASOCIADOS S.A.',
	'J&M DISTRIBUTION S.A.S.',
	'CRIYA S. A.',
	'BIO SAS',
	'EDUCACION COLOMBIA SAS',
	'CRIADERO LA CUMBRE YJ S.A.S',
	'PHONETIFY SAS',
	'INTUKANA S.A.S',
	'STUDIO 4 S.A',
	'GREEN PERFORMACE S.A.S',
	'COOPERATIVA DE TRANSPORTES VELOTAX LIMITADA',
	'PHRONESIS SAS',
	'CLICK2BUY SAS',
	'MEDPLUS CENTRO DE RECUPERACION INTEGRAL SAS',
	'SU PRESENCIA PRODUCCIONES LTDA',
	'TOTAL GP FY 23') then 'gerenciado' else 'masivo' end as gerenciado,
t2.update_
into union_vip_upgrade
from union_ac_ch_cr_2 t1
left join up_down t2 on t1.nombre_merchant = t2.name_merchant and t1.mes = t2.fecha
--limit 5
;
----------------
-- añadiendo columna para contar primera trx sin importar
drop table if exists union_vip_upgrade_fix;
select *
, case when mes= primera_trx_nombre then 'primera_sin_importar' else null end as primera_trx_sin
into union_vip_upgrade_fix
from union_vip_upgrade
--limit 200
;
----------------------------------------





drop table if exists sign_up_variables;
select distinct mail as nombre_merchant,
cast(left("signup date", 10) as date) as mes,
null as inactividad ,
null as tpt ,
null as tpv_usd, 
null as revenue_usd ,
null as gm_usd ,
null as credit_card ,
null as bank_referenced ,
null as referenced ,
null as bank_transfer ,
null as pse ,
null as debit_card ,
null as cash ,
null as cash_on_delivery ,
null as ach ,
null as lending ,
null as suma_de_metodos ,
null as inactivo ,
null as fecha_creacion_nombre ,
null as primera_trx_nombre ,
null as creacion_hasta_first_trx ,
null as mob_first_trx ,
null as mob_creacion_cuenta ,
null as created_activated ,
null as mes_anterior ,
null as back_from_churn ,
null as saldo_promedio_mes ,
null as saldo_f_mes ,
null as monto_fraude ,
null as cantidad_fraude ,
mcc_code as codigo_mcc_id,
null as modelo_pagos,
'OB2' as fuente_creacion,
null as referido,
null as ciudad,
null as city,
null as plana_visa,
null as porcentual_visa,
null as plana_mc,
null as porcentual_mc,
null as plana_pse,
null as porcentual_pse,
null as plana_efecty,
null as porcentual_efecty,
/*cast(recencia as bigint ),
cast(alt_baj_recencia as character varying(4) ),
cast(frecuencia_internet as bigint ),
cast(alt_baj_frecuencia as character varying(4) ),
cast(monto as numeric(38,4) ),
cast(alt_baj_monto as character varying(4) ),
cast(rfm_group as character varying(11) ),*/
'sign_up' as estado_ --as character varying(7) )
, null as gerenciado, null as update_, null as primera_trx_sin
into sign_up_variables --creadas_flag_corregida_cast
from your_table_nlv_april_21 --creadas_flag_corregida
where account is null
--limit 5
;

drop table if exists sign_up_variables_fix;
select distinct  nombre_merchant,
 date(date_trunc('month',mes)) as mes,
cast(inactividad as bigint ),
cast(tpt as double precision ),
cast(tpv_usd as double precision ),
cast(revenue_usd as double precision ),
cast(gm_usd as double precision ),
cast(credit_card as bigint ),
cast(bank_referenced as bigint ),
cast(referenced as bigint ),
cast(bank_transfer as bigint ),
cast(pse as bigint ),
cast(debit_card as bigint ),
cast(cash as bigint ),
cast(cash_on_delivery as bigint ),
cast(ach as bigint ),
cast(lending as bigint ),
cast(suma_de_metodos as bigint ),
cast(inactivo as integer ),
cast(fecha_creacion_nombre as date ),
cast(primera_trx_nombre as date ),
cast(creacion_hasta_first_trx as bigint ),
cast(mob_first_trx as bigint ),
cast(mob_creacion_cuenta as bigint ),
cast(created_activated as character varying(17) ),
cast(mes_anterior as date ),
cast(back_from_churn as character varying(7) ),
cast(saldo_promedio_mes as numeric(38,18) ),
cast(saldo_f_mes as numeric(38,18) ),
cast(monto_fraude as numeric(38,18) ),
cast(cantidad_fraude as bigint ),
cast (codigo_mcc_id as varchar(54)),
modelo_pagos,
'OB2' as fuente_creacion,
 referido ,
ciudad,
city,
cast(plana_visa as numeric(14,2)),
cast(porcentual_visa as numeric(14,2)),
cast(plana_mc as numeric(14,2)),
cast(porcentual_mc as numeric(14,2)),
cast(plana_pse as numeric(14,2)),
cast(porcentual_pse as numeric(14,2)),
cast(plana_efecty as numeric(14,2)),
cast(porcentual_efecty as numeric(14,2)),
/*cast(recencia as bigint ),
cast(alt_baj_recencia as character varying(4) ),
cast(frecuencia_internet as bigint ),
cast(alt_baj_frecuencia as character varying(4) ),
cast(monto as numeric(38,4) ),
cast(alt_baj_monto as character varying(4) ),
cast(rfm_group as character varying(11) ),*/
cast(estado_ as character varying(7) )
, gerenciado, update_, primera_trx_sin
into sign_up_variables_fix--creadas_flag_corregida_cast
from sign_up_variables--creadas_flag_corregida
--limit 5
;



/*select pg_get_cols('union_vip_upgrade_fix')
;
select pg_get_cols('sign_up_variables_fix')
;*/

drop table if exists union_signups;
select *
into union_signups
from (
select *
from union_vip_upgrade_fix
union 
select *
from sign_up_variables_fix);



-- agrupacion para resumen
drop table if exists Pre_dashboard_test;
select mes
, count(CASE WHEN estado_ = 'Activos' then 1 end) as registros
 , count(back_from_churn) as back_from_churn_
 , count(created_activated) as trx_mes_creacion
 , sum(inactivo) as inactivo_
, sum(t1.tpt) as tpt_ , sum(t1.tpv_usd) as tpv_usd_, sum(revenue_usd) as revenue_usd_, sum(gm_usd) as gm_usd_
, sum(suma_de_metodos) as suma_de_metodos_,
 sum(saldo_promedio_mes) as saldo_promedio_mes_sum, sum(saldo_f_mes) as saldo_f_mes_sum
 , sum(monto_fraude) as monto_fraude_sum, sum(cantidad_fraude) as cantidad_fraude_sum
-- priemra_trx_nombre es variable de cuantos transaron sin importar el mes de creación por primera vez
, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion, referido_cuenta_linea,  t1.city
, t1.plana_visa, t1.porcentual_visa, t1.plana_mc, t1.porcentual_mc, t1.plana_pse
, t1.porcentual_pse, t1.plana_efecty, t1.porcentual_efecty
 , count(CASE WHEN estado_ = 'Churn' then 1 end) as churn_numero
  , count(CASE WHEN estado_ = 'Creada' then 1 end) as creados_numero -- es creados sin trx
  , count(CASE WHEN primera_trx_sin = 'primera_sin_importar' and estado_ = 'Activos' then 1 end) as primera_trx
 ,  count(CASE WHEN estado_ = 'sign_up' then 1 end) as signups
  , gerenciado, update_
into Pre_dashboard_test
from union_signups t1--union_signups t1
--where estado_ = 'Activos'
group by mes, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion, referido_cuenta_linea,  t1.city
, t1.plana_visa, t1.porcentual_visa, t1.plana_mc, t1.porcentual_mc, t1.plana_pse
, t1.porcentual_pse, t1.plana_efecty, t1.porcentual_efecty, gerenciado, update_
order by mes
--limit 5
;


/*select *
from pre_dashboard_test
where mes >= '2019-05-01'
order by mes 
--where mes = '2022-05-01'
;*/

-- se le pega la descripción del mcc y se imprime
select t1.*, t2.descripcion 
from Pre_dashboard_test t1
left join staging.polv4_pps_imp_codigo_mcc t2 on t1.codigo_mcc_id = t2.codigo 
where t1.mes >= '2022-07-01' -- t1.mes >= '2019-04-01'
--limit 200
;



------------------- añadiendo rfm 

drop table if exists activos_nombre_rfm_union;
select t1.*
, t2.recencia, t2.alt_baj_recencia
, t2.frecuencia_internet, t2.alt_baj_frecuencia
, t2.monto, t2.alt_baj_monto
, t2.rfm_group
into activos_nombre_rfm_union
from union_vip_upgrade_fix t1
left join RFM_cruzado_gerenciados t2 on t1.nombre_merchant = t2.nombre 
and t1.mes = date(date_trunc('month', t2.fecha_mes))
--limit 200
;




-- Tabla Resumen RFM
drop table if exists rfm_dashboard;
select count(1), rfm_group, mes, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion
, referido_cuenta_linea,  t1.city, gerenciado, update_
into rfm_dashboard
from activos_nombre_rfm_union t1
where mes >= '2022-07-01' and estado_ = 'Activos'
group  by  rfm_group, mes, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion
, referido_cuenta_linea,  t1.city, gerenciado, update_
;

select *
from rfm_dashboard
;



-- para matriz de rodamiento
select *
, case when
lag(t1.nombre_merchant, 1) over(order by t1.nombre_merchant asc, t1.mes asc) = t1.nombre_merchant 
then
lag(t1.rfm_group, 1) over(order by t1.nombre_merchant asc, t1.mes asc) 
else null
end as last_rfm
from activos_nombre_rfm_union t1
where mes >= '2022-07-01' and estado_ = 'Activos'
order by t1.nombre_merchant asc, t1.mes asc
--limit 200
;
-----------------------------------------------------------------------------------------------------------------------
-----
--
--
--

-------------------------
--para pestañas del dashboard  de mobs

-- real (nuevo) listo
--resumen economico con fecha de primera trx -- se imprime, pestaña 'Mobs first trx (Avg)'
select primera_trx_nombre  ,  mob_first_trx ,  avg(tpt) as tpt
, avg(tpv_usd) as tpv, avg(revenue_usd) as revenue, count(1) as cantidad
, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion, referido_cuenta_linea,  t1.city
, t1.plana_visa, t1.porcentual_visa, t1.plana_mc, t1.porcentual_mc, t1.plana_pse
, t1.porcentual_pse, t1.plana_efecty, t1.porcentual_efecty, gerenciado, update_
from activos_nombre_rfm_union t1
where mes >= '2019-05-01' and estado_ = 'Activos'
group by primera_trx_nombre, mob_first_trx 
, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion, referido_cuenta_linea,  t1.city
, t1.plana_visa, t1.porcentual_visa, t1.plana_mc, t1.porcentual_mc, t1.plana_pse
, t1.porcentual_pse, t1.plana_efecty, t1.porcentual_efecty, gerenciado, update_
order by primera_trx_nombre desc, mob_first_trx asc
--limit 200
;



--- equivalente pero con la fecha de creación
select fecha_creacion_nombre  ,  mob_creacion_cuenta ,  avg(tpt) as tpt
, avg(tpv_usd) as tpv, avg(revenue_usd) as revenue, count(1) as cantidad
, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion, referido_cuenta_linea,  t1.city
, t1.plana_visa, t1.porcentual_visa, t1.plana_mc, t1.porcentual_mc, t1.plana_pse
, t1.porcentual_pse, t1.plana_efecty, t1.porcentual_efecty, gerenciado, update_
from activos_nombre_rfm_union t1
where mes >= '2019-05-01' and estado_ = 'Activos'
group by fecha_creacion_nombre, mob_creacion_cuenta 
, t1.codigo_mcc_id, t1.modelo_pagos, t1.fuente_creacion, referido_cuenta_linea,  t1.city
, t1.plana_visa, t1.porcentual_visa, t1.plana_mc, t1.porcentual_mc, t1.plana_pse
, t1.porcentual_pse, t1.plana_efecty, t1.porcentual_efecty, gerenciado, update_
order by fecha_creacion_nombre desc, mob_creacion_cuenta asc
--limit 200
;






