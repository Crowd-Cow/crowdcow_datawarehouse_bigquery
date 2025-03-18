with

source as ( select * from {{ source('shareasale', 'transaction_details') }} )

,renamed as (
    select
        transID as transaction_id,
        userID as affiliate_id,
        timestamp(transdate) as transaction_date_utc,
        transamount as transaction_amount,
        commission as comission,
        ssamount as shareasale_comission,
        comment,
        voided,
        locked,
        pending,
        lastip,
        lastreferer,
        bannernumber,
        bannerpage,
        dateoftrans,
        dateofclick,
        timeofclick,
        dateofreversal,
        returndays,
        toolID,
        storeID,
        lockDate,
        transactionType,
        CommissionType,
        skulist,
        priceList,
        quantityList,
        orderNumber as order_token,
        parentTrans,
        bannerName,
        bannerType,
        couponCode,
        referenceTrans,
        newCustomerFlag as new_customer_flag,
        userAgent,
        originalCurrency,
        originalCurrencyAmount,
        isMobile,
        usedACoupon,
        if(merchantDefinedType = 'alacarte',true,false) as is_alacarte_order,
        row_number() over ( partition by transID order by transdate desc) as rn
    from source
)

select * from renamed where rn = 1 
