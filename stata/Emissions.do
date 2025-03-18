/*==============================================================================
 EMISSIONS CALCULATION FOR INDONESIAN MANUFACTURING FIRMS
 Description: This do-file calculates CO2 emissions from various energy sources
              using Indonesian manufacturing survey data (IBS)
==============================================================================*/

* Set working directory
global workdir "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau"

/*------------------------------------------------------------------------------
 SECTION 1: Data Preparation and Merging
------------------------------------------------------------------------------*/

* 1.1 Import and prepare IBS dataset
use "/Users/schalkeanindya/Downloads/ibs2021.dta", clear
rename _all, upper
gen DISIC2 = substr(DISIC521, 1, 2)

* Convert variables to string for merging
local string_vars DISIC2 LTLOFF21 LPRNOL21 LPRNOF21 LNPNOL21 LNPNOF21 ///
                  LTLMHS21 LTLSHS21 LTLMDI21 LTLMSM21 LTLNOU21 LTLRND21
tostring `string_vars', replace

* Create merger ID and remove duplicates
gen merger = DISIC2 + LTLOFF21 + LPRNOL21 + LPRNOF21 + LNPNOL21 + LNPNOF21 + ///
            LTLMHS21 + LTLSHS21 + LTLMDI21 + LTLMSM21 + LTLNOU21 + LTLRND21
duplicates drop merger, force

tempfile ibs
save `ibs', replace

* 1.2 Import and prepare RGS dataset
import dbase "/Users/schalkeanindya/Downloads/803/ibs21_kbli_1_diseminasi_digit2.dbf", clear
rename _all, upper

* Prepare RGS data for merging
tostring `string_vars', replace
gen merger = DISIC2 + LTLOFF21 + LPRNOL21 + LPRNOF21 + LNPNOL21 + LNPNOF21 + ///
            LTLMHS21 + LTLSHS21 + LTLMDI21 + LTLMSM21 + LTLNOU21 + LTLRND21
duplicates drop merger, force

* Merge datasets
merge 1:1 DISIC2 merger using `ibs', keep(3) nogen
save "$workdir/IBS_21", replace

/*------------------------------------------------------------------------------
 SECTION 2: Energy Conversion Factors
------------------------------------------------------------------------------*/

use "$workdir/IBS_21", clear

* 2.1 Define energy conversion factors (TJ per unit)
global TJ_EPELIU 0.00003315  // Gasoline (TJ/liter)
global TJ_ESOLIU 0.000037    // Solar (TJ/liter)
global TJ_ESDLIU 0.000037    // Diesel oil (TJ/liter)
global TJ_EDILIU 0.000037    // Bio diesel (TJ/liter)
global TJ_ECLKGU 0.0000187   // Coal (TJ/kg)
global TJ_ECBKGU 0.0000189   // Coal briquettes (TJ/kg)
global TJ_ENGKGU 0.000036    // Natural gas (TJ/MMBTU)
global TJ_EFOLIU 0.000040    // Fuel oil (TJ/L)
global TJ_ELPKGU 0.000046    // LPG (TJ/kg)
global TJ_ECAKGU 0.0000156   // Biomass (TJ/kg)
global TJ_ELULIU 0.000038    // Lubricant (TJ/liter)
global KWH 277777.78         // Conversion factor to KWH

/*------------------------------------------------------------------------------
 SECTION 3: Calculate Energy Content
------------------------------------------------------------------------------*/

* Generate energy content in Terajoules for each fuel type
local energy_types EPELIU ESOLIU ESDLIU EDILIU ECLKGU ECBKGU ENGKGU EFOLIU ELPKGU ECAKGU ELULIU
foreach type in `energy_types' {
    gen TJ_`type' = `type' * ${TJ_`type'} if `type' != .
}

/*------------------------------------------------------------------------------
 SECTION 4: Emission Factors and CO2 Calculations
------------------------------------------------------------------------------*/

* 4.1 Define emission factors (ton CO2 per TJ)
global EF_EPELIU 69.29   // Gasoline
global EF_ESOLIU 72.93   // Solar
global EF_ESDLIU 74.52   // Diesel oil
global EF_EDILIU 74.52   // Bio diesel
global EF_ECLKGU 0.11    // Coal
global EF_ECBKGU 0.096   // Coal briquettes
global EF_ENGKGU 56.1    // Natural gas
global EF_EFOLIU 77.9    // Fuel oil
global EF_ELPKGU 65.4    // LPG
global EF_ECAKGU 1.7472  // Biomass
global EF_ELULIU 73.3    // Lubricant
global EF_median 59.29   // Other energy sources

* 4.2 Calculate CO2 emissions for each fuel type
foreach type in `energy_types' {
    gen CO2_`type' = TJ_`type' * ${EF_`type'} if `type' != .
}

/*------------------------------------------------------------------------------
 SECTION 5: Calculate Energy Costs and Other Metrics
------------------------------------------------------------------------------*/

* 5.1 Calculate value per TJ for each energy type
gen EPEVCU21_PER_ENERGY = EPEVCU21 / TJ_EPELIU
gen ESOVCU21_PER_ENERGY = ESOVCU21 / TJ_ESOLIU
gen ESDVCU21_PER_ENERGY = ESDVCU21 / TJ_ESDLIU
gen EDIVCU21_PER_ENERGY = EDIVCU21 / TJ_EDILIU
gen ECLVCU21_PER_ENERGY = ECLVCU21 / TJ_ECLKGU
gen ECBVCU21_PER_ENERGY = ECBVCU21 / TJ_ECBKGU
gen ENGVCU21_PER_ENERGY = ENGVCU21 / TJ_ENGKGU
gen EFOVCU21_PER_ENERGY = EFOVCU21 / TJ_EFOLIU
gen ELPVCU21_PER_ENERGY = ELPVCU21 / TJ_ELPKGU
gen ECAVCU21_PER_ENERGY = ECAVCU21 / TJ_ECAKGU
gen ELUVCU21_PER_ENERGY = ELUVCU21 / TJ_ELULIU

* 5.2 Calculate other energy emissions
egen meancost = rowmean(*_PER_ENERGY)
gen TJ_OTHERENERGY = ENCVCU / meancost
gen CO2_OTHERENERGY = TJ_OTHERENERGY * ${EF_EPELIU}

* 5.3 Convert energy to KWH
foreach var of varlist TJ_* {
    local basename = substr("`var'", 4, .)
    gen KWH_`basename' = `var' * ${KWH} if `var' != .
}

/*------------------------------------------------------------------------------
 SECTION 6: Calculate Totals and Value Added
------------------------------------------------------------------------------*/

* 6.1 Calculate total emissions and energy
egen TOTAL_CO2 = rowtotal(CO2_*), missing
egen TOTAL_TJ = rowtotal(TJ_*), missing
gen TOTAL_KWH = TOTAL_TJ * ${KWH} if TOTAL_TJ != .

* 6.2 Calculate value added
gen r1002 = YPRVCU + YRSVCU + (YRNVCU - NOPVCU) + (STDVCU - STJVCU)
gen r1003 = ILRVCU + ITXVCU + IINVCU + ICOVCU + IDEVCU + IPRVCU
gen r1001 = ZPDVCU + ZNDVCU + EFUVCU + EPLVCU + ENPVCU + IOTVCU + RDNVCU + RIMVCU
gen r1001a = ZPDVCU + ZNDVCU
gen VTLVCU = r1002 - r1001 + r1001a + r1003

* 6.3 Calculate emission intensity
gen intensity = TOTAL_CO2 / VTLVCU

* Add variable labels
label variable TOTAL_CO2 "Total CO2 emissions (kg CO2)"
label variable TOTAL_TJ "Total energy content (TJ)"
label variable TOTAL_KWH "Total energy content (kWh)"
label variable intensity "CO2 emissions per value added"

* Save final dataset
save "$workdir/CO2_EMISSION", replace

* Display summary statistics
summarize CO2_* TJ_* KWH_* TOTAL_CO2 TOTAL_TJ TOTAL_KWH intensity 