with source as (

  select * from {{ source('raw', 'test') }}

)

,final as (
	select * from source
)
select * from final
