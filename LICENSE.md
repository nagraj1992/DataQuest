/******************************************************************************
PROGRAM:    ld_asrs_part_hub_attributes.r
PURPOSE:    Provide mass load capabilities for ASRS updates.
*******************************************************************************/
#include <db/usr1/inventory/stock.hpp>
#include <db/usr1/inventory/stock_ext.hpp>
#include <db/usr1/inventory/hubs.hpp>
#include <db/usr1/inventory/inv_status_rqid.hpp>
#include <db/usr7/sr/asrs_partinfo.hpp>
#include <db/usr1/inventory/warehsloc.hpp>
#include <db/usr1/inventory/warehslocx.hpp>
#include <db/usr1/inventory/whslocaux.hpp>
#include <db/usr0/menu/hubs/general/hub_specific.hpp>
#include <db/usr1/inventory/captive_container.hpp>
#include <db/usr7/cs/hazmat.hpp>
#include <db/usr17/sr/tf_container_empty.hpp>
#include <db/usr2/inv/stock_ext_desc.hpp>
#include <db/usr5/ap/cc4to5.hpp>

#define SAGEREP_UNLOCKED_DRIVING_FILE stock
// Library includes
#define NO_SAGEREP_PARAMS
#define SAGEREP_USE_INIT
#include <sculptor/sagerep_main.hpp>

#include "lib/deprecated/getmtime.hpp"
#include "lib/deprecated/intf_wms_loc_inv.hpp"
#include "lib/deprecated/hub_specific.hpp"
#include "lib/utility.hpp"

#include <view/report.hpp>
using namespace view::report;
#include <iostream>
using std::endl;
#include <string>
using std::string;
#include <lib/string_util.hpp>
namespace su = lib::string_util;
#include <glog/logging.h>

string hub; /* a4, l+ */
string part; /* a15 */
double full_tote_qty; /* r8 */
string velocity; /* a1, u+ */
string nonconveyable; /* a1, u+ */
string liquid; /* a1, u+ */
string tempsens; /* a1, u+ */
string chkBadFormat; /* a10 */
DEFINE_FIELD(long, totalcntr, "totalcntr", "######0"); /* i4 */
DEFINE_FIELD(long, errcntr, "errcntr", "######0"); /* i4 */
DEFINE_FIELD(long, updcntr, "updcntr", "######0"); /* i4 */
DEFINE_FIELD(string, inFile, "inFile", 70); /* a70 */
string outFile; /* a70 */
string errFile; /* a70 */
DEFINE_FIELD(string, currentFile, "currentFile", 70); /* a70 */
string histFile; /* a70 */
string taskid; /* a5 */
string run_or_end_it; /* a1, u+ */
string sculptor_ctime; /* a8 */ /* variables for getmtime.lib  */
string h1; /* a1 */
string h2; /* a1 */
string m1; /* a1 */
string m2; /* a1 */
string s1; /* a1 */
string s2; /* a1 */
long seconds; /* i2 */
long minutes; /* i2 */
long hours; /* i2 */ /* end of time variables */
string svseparator; /* a1 */
short i; /* i2 */

DEFINE_FIELD_FOR_EXISTING(short, sculptor_errno, "", "###0");

void sculptor_main();
void show_summary();
void sync_asrs_container_content();

// Function definitions
void sagerep_init()
{
    view::report::set_sculptor_compatible_mode();
    {
        sculptor_main();
        show_summary();
        exit(EXIT_SUCCESS);
    }
}

void sagerep_main()
{
    sculptor_main(); /* TODO: remove fall-through if unreachable */
}


void sculptor_main()
{
    hub = tolower(arg[4]);
    if (hub == "") {
        input("Enter 4-letter hub code", &hub);
        hub = tolower(hub);
    }
    clearbuf(hubs);
    hubs->stash_buffer();
    hb_code = hub;
    try {
        readu(hubs);
    } catch (nsr&) {
        hubs->restore_buffer();
        goto err_hub;
    }
    clearbuf(cc4to5);
    cc4to5->stash_buffer();
    cc4to5_char4 = hub;
    try {
        readu(cc4to5);
    } catch (nsr&) {
        cc4to5->restore_buffer();
        goto err_hub;
    }
    if (0) {
err_hub:
        print() << "Invalid hub, exiting now..." << endl;
        sleep(3);
        exit(EXIT_SUCCESS);
    }
    display("--------------------------------------------------------------------------------------------");
    display("Input filename: asrsPartHub_" / hub / ".csv (in massload share)");
    display("");
    display("Data format: Part, Full Tote Qty, Velocity, Non-Conveyable Flag, Liquid, Temp Sensitive");
    display("");
    display("Full Tote Qty value cannot be greater than 15 digits");
    display("");
    display("DO NOT INCLUDE ANY COLUMN HEADERS IN THIS FILE");
    display("");
    display("--------------------------------------------------------------------------------------------");
    input("Ready to run (Y/N): ", &run_or_end_it);
    run_or_end_it = toupper(run_or_end_it);
    if (run_or_end_it == "N") {
        exit(EXIT_SUCCESS);
    }
    taskid = task();
    i = sculptor_cast<short>(sculptor::chdir("/share/oper1a/massload"));
    if (i != 0) {
        i = sculptor_cast<short>(sculptor::chdir("/share/oper2a/massload"));
    }
    if (i != 0) {
        i = sculptor_cast<short>(sculptor::chdir("/share/artemis/massload"));
    }
    if (i != 0) {
        tstat = 1;
        exit(EXIT_SUCCESS);
    }
    inFile = "asrsPartHub_" / hub / ".csv";
    outFile = "asrsPartHub_" / hub / ".out";
    errFile = "asrsPartHub_" / hub / ".err";
    histFile = "/tmp/asrsPartHub_" / user() / taskid;
    exec("test -r " + inFile);
    if (tstat) {
        print() << "Cannot open input file: " + F(inFile) << endl;
        sleep(5);
        exit(EXIT_SUCCESS);
    }
    exec("/usr/bin/dos2unix " + inFile);
    if (tstat) {
        print() << "Cannot convert input file to unix format: " + F(inFile) << endl;
        sleep(5);
        exit(EXIT_SUCCESS);
    }
    currentFile = inFile;
    try {
        open(3, inFile, read_mode);
    } catch (err&) {
        goto OpenErr;
    }
    currentFile = outFile;
    try {
        open(4, outFile, write_mode);
    } catch (err&) {
        goto OpenErr;
    }
    currentFile = errFile;
    try {
        open(5, errFile, write_mode);
    } catch (err&) {
        goto OpenErr;
    }
    exec("cp " + su::rpad(inFile, 70) + " " + histFile);
    print() << "Processing updates..." << endl;
    LOG(INFO) << "Processing updates...";
    while (1) {
        try {
            get(3) >> part >> full_tote_qty >> velocity >> nonconveyable >> liquid >> tempsens >> chkBadFormat;
        } catch (err&) {
            goto GetErr;
        }
        LOG(INFO) << "NEXT PART " << part;
        velocity = toupper(velocity);
        nonconveyable = toupper(nonconveyable);
        liquid = toupper(liquid);
        tempsens = toupper(tempsens);
        totalcntr = totalcntr + 1;
        /* Validate all fields of input */
        if (chkBadFormat != "") {
            LOG(INFO) << "BAD FORMAT " << chkBadFormat;
            goto err_format;
        }
        clearbuf(stock);
        stock->stash_buffer();
        inv_part = part;
        try {
            readu(stock);
        } catch (nsr&) {
            stock->restore_buffer();
            goto err_part;
        }

        /* Floating point field stored internally in IEEE floating point format with up to 15 significant digits */
        if (full_tote_qty > 999999999999999.0) {
            LOG(INFO) << "full tote qty " << full_tote_qty;
            goto err_fulltoteqty;
        }
        LOG(INFO) << "NEXT PART " << part;
        if (hub == "ihub") {
            if (velocity != "A" && velocity != "B" && velocity != "C") {
                goto err_velocity;
            }
        } else {
            if (velocity < "A" || velocity > "E") {
                goto err_velocity;
            }
        }
        LOG(INFO) << "NEXT PART " << part;
        if (nonconveyable != "Y" && nonconveyable != "N") {
            goto err_nonconveyable;
        }
        LOG(INFO) << "NEXT PART " << part;
        if (liquid != "Y" && liquid != "N") {
            goto err_liquid;
        }
        LOG(INFO) << "NEXT PART " << part;
        if (tempsens != "Y" && tempsens != "N") {
            goto err_tempsens;
        }
        LOG(INFO) << "NEXT PART " << part;
        clearbuf(stock_ext);
        inve_part = part;
        if (1) {
            try {
                read(stock_ext);
            } catch (nsr&) {
                goto ins_stock_ext;
            }
            inve_liquid = toupper(liquid);
            inve_tempsense = toupper(tempsens);
            try {
                write(stock_ext);
            } catch (nrs&) {
                LOG(INFO) << "No record selected in stock_ext" << LOG_STOCK_EXT_FIELDS;
            } catch (re&) {
                LOG(INFO) << "Record already exists in stock_ext" << LOG_STOCK_EXT_FIELDS;
            }
        } else {
ins_stock_ext:
            inve_liquid = toupper(liquid);
            inve_tempsense = toupper(tempsens);
            try {
                insert(stock_ext);
            } catch (re&) {
                LOG(INFO) << "Record already exists in stock_ext" << LOG_STOCK_EXT_FIELDS;
            }
        }
        LOG(INFO) << "NEXT PART " << part;
        clearbuf(asrs_partinfo);
        asrp_hub = tolower(hub);
        asrp_part = part;
        if (1) {
            try {
                read(asrs_partinfo);
            } catch (nsr&) {
                goto ins_asrs_partinfo;
            }
            asrp_toteqty = full_tote_qty;
            asrp_nonconveyable = toupper(nonconveyable);
            asrp_velocity = toupper(velocity);
            try {
                write(asrs_partinfo);
            } catch (nrs&) {
                LOG(INFO) << "No record selected in asrs_partinfo" << LOG_ASRS_PARTINFO_FIELDS;
            } catch (re&) {
                LOG(INFO) << "Record already exists in asrs_partinfo" << LOG_ASRS_PARTINFO_FIELDS;
            }
        } else {
ins_asrs_partinfo:
            asrp_hub = tolower(hub);
            asrp_part = part;
            asrp_toteqty = full_tote_qty;
            asrp_nonconveyable = toupper(nonconveyable);
            asrp_velocity = toupper(velocity);
            try {
                insert(asrs_partinfo);
            } catch (re&) {
                LOG(INFO) << "Record already exists in asrs_partinfo" << LOG_ASRS_PARTINFO_FIELDS;
            }
        }
        sync_asrs_container_content();
        put(4) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "UPDATED SUCCESSFULLY" << endl;
        updcntr = updcntr + 1;
        continue;
err_format:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID FORMAT - EXTRA FIELDS" << endl;
        errcntr = errcntr + 1;
        continue;
err_part:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID PART" << endl;
        errcntr = errcntr + 1;
        continue;
err_fulltoteqty:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID QTY" << endl;
        errcntr = errcntr + 1;
        continue;
err_velocity:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID VELOCITY" << endl;
        errcntr = errcntr + 1;
        continue;
err_nonconveyable:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID NC FLAG" << endl;
        errcntr = errcntr + 1;
        continue;
err_liquid:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID LIQUID" << endl;
        errcntr = errcntr + 1;
        continue;
err_tempsens:
        put(5) << su::rpad(hub, 4) << su::rpad(part, 15) << tostr(full_tote_qty, "########################") << su::rpad(velocity, 1) << su::rpad(nonconveyable, 1) << su::rpad(liquid, 1) << su::rpad(tempsens, 1) << "INVALID TEMP SENS" << endl;
        errcntr = errcntr + 1;
        continue;
    }
    return;
OpenErr:
    print() << F(currentFile) << endl;
    switch (sculptor_errno) {
        case 1: {
            print() << "" << endl;
            break;
        }
        case 2: {
            print() << "" << endl;
            break;
        }
        case 3: {
            print() << "" << endl;
            break;
        }
        default: {
            print() << "Error opening file: " << F(sculptor_errno) << endl;
            break;
        }
    }
    exit(EXIT_SUCCESS);
GetErr:
    if (sculptor_errno == FEOF) {
        print() << "Completed" << endl;
    } else {
        print() << "Get Error " << F(sculptor_errno) << endl;
    }
    {
        sculptor::close(3);
        sculptor::close(4);
        sculptor::close(5);
    }
    return;
}

void sync_asrs_container_content()
{
    LOG(INFO) << "SYNCING";
    asrs_USER_NAME = user();
    asrs_PROGRAM_NAME = "ld_asrs_part_hub_attribut";
    asrs_BUSINESS_UNIT = cc4to5_char5;

    clearbuf(hub_specific);
    hubSpecific_hub = tolower(hub);
    hubSpecific_pgm = "TOTE_FLOW";
    hubSpecific_function = 0;
    hubSpecificCheck();
    /* lib/hub_specific.lib */
    tote_flow_active = toupper(hubSpecific_active);

    clearbuf(warehslocx);
    hseqx_hub = tolower(hub);
    hseqx_part = part;
    try {
        findu(warehslocx);
    } catch (nsr&) {
        goto nsr_hseqx;
    }
    if (0) {
nsr_hseqx:
        return;
    }
    svseparator = separator;
    separator = "|";
    while (hseqx_hub == hub && hseqx_part == part) {
        clearbuf(captive_container);
        ccont_hub = tolower(hseqx_hub);
        ccont_id = toupper(hseqx_grp);
        if (1) {
            try {
                readu(captive_container);
            } catch (nsr&) {
                goto breakif_sync_asrs_container_content0;
            }
breakif_sync_asrs_container_content0:
            ;
        }
        if (ccont_type == "ML" || ccont_type == "UL") {
            asrs_CONTAINER_ID = hseqx_grp;
            wms_PARTNO = hseqx_part;
            if (lib::utility::evaluate_flag(tote_flow_active)) {
                calc_tote_flow_capable_flag();
                upd_tote_flow_capable_flag();
            }
            upd_intf_ASRS_container_content();
            /* lib/intf_wms_loc_inv.lib */
            reset_warehsloc_row();
            intf_container_content();
        }
        try {
            matchu(warehslocx);
        } catch (nsr&) {
            break;
        }
    }
    separator = svseparator;
    return;
}

void show_summary()
{
    print() << "Total Records:" << tab(18) << F(totalcntr) << endl;
    print() << "Total Errors:" << tab(18) << F(errcntr) << endl;
    print() << "Total Updates:" << tab(18) << F(updcntr) << endl;
    input("Press enter to end ", &run_or_end_it);
    run_or_end_it = toupper(run_or_end_it);
    return;
}
