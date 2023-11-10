with source as (

  select * from {{ source('main', 'test') }}

)

,final as (
	select * from source
)
select * from final

