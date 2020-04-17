use rust_decimal::Decimal;
use num::ToPrimitive;

pub fn to_fix(mut number:f64,mut precision:u32) -> f64{
    let times = 10_u32.pow(precision);
    let number_tmp = number * times as f64;
    let real_number = number_tmp.round();
    let decimal_number = Decimal::new(real_number as i64, precision);
    let scaled = decimal_number.to_f64().unwrap();
    println!("to_fix-{}--",scaled);
    scaled
}