#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <zmq.h>

#include <pnet_api.h>
#include "osal.h"
#include "log.h"

/********************* Call-back function declarations ************************/

static int app_exp_module_ind(uint16_t api, uint16_t slot, uint32_t module_ident_number);
static int app_exp_submodule_ind(uint16_t api, uint16_t slot, uint16_t subslot, uint32_t module_ident_number, uint32_t submodule_ident_number);
static int app_new_data_status_ind(uint32_t arep, uint32_t crep, uint8_t changes);
static int app_connect_ind(uint32_t arep, pnet_result_t *p_result);
static int app_state_ind(uint32_t arep, pnet_event_values_t state);
static int app_release_ind(uint32_t arep, pnet_result_t *p_result);
static int app_dcontrol_ind(uint32_t arep, pnet_control_command_t control_command, pnet_result_t *p_result);
static int app_ccontrol_cnf(uint32_t arep, pnet_result_t *p_result);
static int app_write_ind(uint32_t arep, uint16_t api, uint16_t slot, uint16_t subslot, uint16_t idx, uint16_t sequence_number, uint16_t write_length, uint8_t *p_write_data, pnet_result_t *p_result);
static int app_read_ind(uint32_t arep, uint16_t api, uint16_t slot, uint16_t subslot, uint16_t idx, uint16_t sequence_number, uint8_t **pp_read_data, uint16_t *p_read_length, pnet_result_t *p_result);
static int app_alarm_cnf(uint32_t arep, pnet_pnio_status_t *p_pnio_status);
static int app_alarm_ind(uint32_t arep, uint32_t api, uint16_t slot, uint16_t subslot, uint16_t data_len, uint16_t data_usi, uint8_t *p_data);
static int app_alarm_ack_cnf(uint32_t arep, int res);


/********************** Settings **********************************************/

#define EVENT_READY_FOR_DATA           BIT(0)
#define EVENT_TIMER                    BIT(1)
#define EVENT_ALARM                    BIT(2)
#define EVENT_ABORT                    BIT(15)

#define EXIT_CODE_ERROR                1
#define TICK_INTERVAL_US               1000        /* 1 ms */
#define APP_DEFAULT_ETHERNET_INTERFACE "eth0"
#define APP_PRIORITY                   15
#define APP_STACKSIZE                  4096        /* bytes */
#define APP_MAIN_SLEEPTIME_US          5000*1000
#define PNET_MAX_OUTPUT_LEN            256

#define IP_INVALID                     0


/**************** From the GSDML file ****************************************/

#define APP_DEFAULT_STATION_NAME       "historianio"
#define APP_API                        0

/*
 * Module and submodule ident number for the DAP module.
 * The DAP module and submodules must be plugged by the application after the call to pnet_init.
 */
#define PNET_SLOT_DAP_IDENT                        0x00000000
#define PNET_MOD_DAP_IDENT                         0x00000001     /* For use in slot 0 */
#define PNET_SUBMOD_DAP_IDENT                      0x00000001     /* For use in subslot 1 */
#define PNET_SUBMOD_DAP_INTERFACE_1_IDENT          0x00008000     /* For use in subslot 0x8000 */
#define PNET_SUBMOD_DAP_INTERFACE_1_PORT_0_IDENT   0x00008001     /* For use in subslot 0x8001 */

/*
 * I/O Modules. These modules and their sub-modules must be plugged by the
 * application after the call to pnet_init.
 *
 * Assume that all modules only have a single submodule, with same number.
 */
#define PNET_MOD_8B01_IDENT         0x00000100
#define PNET_MOD_8U08_IDENT         0x00000200
#define PNET_MOD_8U16_IDENT         0x00000210
#define PNET_MOD_8U32_IDENT         0x00000220
#define PNET_MOD_8U64_IDENT         0x00000230
#define PNET_MOD_8I08_IDENT         0x00000300
#define PNET_MOD_8I16_IDENT         0x00000310
#define PNET_MOD_8I32_IDENT         0x00000320
#define PNET_MOD_8I64_IDENT         0x00000330
#define PNET_MOD_8F32_IDENT         0x00000420
#define PNET_MOD_8F64_IDENT         0x00000430
#define PNET_SUBMOD_CUSTOM_IDENT    0x00000001


/*** Lists of supported modules and submodules ********/

typedef enum pnet_data_type_values
{
   PNET_DATA_TYPE_NONE                       = 0,
   PNET_DATA_TYPE_BOOL                       = 1,
   PNET_DATA_TYPE_UINT                       = 2,
   PNET_DATA_TYPE_INT                        = 3,
   PNET_DATA_TYPE_FLOAT                      = 4,
} pnet_data_type_values_t;

static const struct
{
   uint32_t                api;
   uint32_t                module_ident_nbr;
   uint32_t                submodule_ident_nbr;
   pnet_submodule_dir_t    data_dir;
   uint16_t                insize;      // total in byte length (here always 0)
   uint16_t                outsize;     // total out byte length (can be calculated as var_count * (2 ^ (var_bitlen - 1)) / 8
   uint32_t                var_type;    // variable type
   uint32_t                var_bitlen;  // variable bit length
   uint32_t                var_count;   // variable count
} cfg_available_submodule_types[] =
{
   {APP_API, PNET_MOD_DAP_IDENT, PNET_SUBMOD_DAP_IDENT, PNET_DIR_NO_IO, 0, 0, PNET_DATA_TYPE_NONE, 0, 0},
   {APP_API, PNET_MOD_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_IDENT, PNET_DIR_NO_IO, 0, 0, PNET_DATA_TYPE_NONE, 0, 0},
   {APP_API, PNET_MOD_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_PORT_0_IDENT, PNET_DIR_NO_IO, 0, 0, PNET_DATA_TYPE_NONE, 0, 0},
   {APP_API, PNET_MOD_8B01_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 1 / 8 * 8, PNET_DATA_TYPE_BOOL, 1, 8},
   {APP_API, PNET_MOD_8U08_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 8 / 8 * 8, PNET_DATA_TYPE_UINT, 8, 8},
   {APP_API, PNET_MOD_8U16_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 16 / 8 * 8, PNET_DATA_TYPE_UINT, 16, 8},
   {APP_API, PNET_MOD_8U32_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 32 / 8 * 8, PNET_DATA_TYPE_UINT, 32, 8},
   {APP_API, PNET_MOD_8U64_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 64 / 8 * 8, PNET_DATA_TYPE_UINT, 64, 8},
   {APP_API, PNET_MOD_8I08_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 8 / 8 * 8, PNET_DATA_TYPE_INT, 8, 8},
   {APP_API, PNET_MOD_8I16_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 16 / 8 * 8, PNET_DATA_TYPE_INT, 16, 8},
   {APP_API, PNET_MOD_8I32_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 32 / 8 * 8, PNET_DATA_TYPE_INT, 32, 8},
   {APP_API, PNET_MOD_8I64_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 64 / 8 * 8, PNET_DATA_TYPE_INT, 64, 8},
   {APP_API, PNET_MOD_8F32_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 32 / 8 * 8, PNET_DATA_TYPE_FLOAT, 32, 8},
   {APP_API, PNET_MOD_8F64_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 64 / 8 * 8, PNET_DATA_TYPE_FLOAT, 64, 8},
};


/************ Configuration of product ID, software version etc **************/


/************ Configuration of product ID, software version etc **************/

static pnet_cfg_t                pnet_default_cfg =
{
      /* Call-backs */
      .state_cb = app_state_ind,
      .connect_cb = app_connect_ind,
      .release_cb = app_release_ind,
      .dcontrol_cb = app_dcontrol_ind,
      .ccontrol_cb = app_ccontrol_cnf,
      .read_cb = app_read_ind,
      .write_cb = app_write_ind,
      .exp_module_cb = app_exp_module_ind,
      .exp_submodule_cb = app_exp_submodule_ind,
      .new_data_status_cb = app_new_data_status_ind,
      .alarm_ind_cb = app_alarm_ind,
      .alarm_cnf_cb = app_alarm_cnf,
      .alarm_ack_cnf_cb = app_alarm_ack_cnf,

      .im_0_data =
      {
       .vendor_id_hi = 0xb0,
       .vendor_id_lo = 0x0d,
       .order_id = "<orderid>           ",
       .im_serial_number = "<serial nbr>    ",
       .im_hardware_revision = 1,
       .sw_revision_prefix = 'P', /* 'V', 'R', 'P', 'U', or 'T' */
       .im_sw_revision_functional_enhancement = 0,
       .im_sw_revision_bug_fix = 0,
       .im_sw_revision_internal_change = 0,
       .im_revision_counter = 0,
       .im_profile_id = 0x1234,
       .im_profile_specific_type = 0x5678,
       .im_version_major = 1,
       .im_version_minor = 1,
       .im_supported = 0x001e,         /* Only I&M0..I&M4 supported */
      },
      .im_1_data =
      {
       .im_tag_function = "",
       .im_tag_location = ""
      },
      .im_2_data =
      {
       .im_date = ""
      },
      .im_3_data =
      {
       .im_descriptor = ""
      },
      .im_4_data =
      {
       .im_signature = ""
      },

      /* Device configuration */
      .device_id =
      {  /* device id: vendor_id_hi, vendor_id_lo, device_id_hi, device_id_lo */
         0xb0, 0x0d, 0x00, 0x01,
      },
      .oem_device_id =
      {  /* OEM device id: vendor_id_hi, vendor_id_lo, device_id_hi, device_id_lo */
         0xc0, 0xff, 0xee, 0x01,
      },
      .station_name = "",              /* Set by command line argument */
      .device_vendor = "pg",
      .manufacturer_specific_string = "Historian Proeinet IO",

      .lldp_cfg =
      {
       .chassis_id = "pg-hist",
       .port_id = "port-001",
       .ttl = 20,          /* seconds */
       .rtclass_2_status = 0,
       .rtclass_3_status = 0,
       .cap_aneg = 3,      /* Supported (0x01) + enabled (0x02) */
       .cap_phy = 0x0C00,  /* Unknown (0x8000) */
       .mau_type = 0x0010, /* Default (copper): 100BaseTXFD */
      },

      /* Network configuration */
      .send_hello = 1,                    /* Send HELLO */
      .dhcp_enable = 0,
      .ip_addr = { 0, 0, 0, 0 },          /* Read from Linux kernel */
      .ip_mask = { 255, 255, 255, 0 },    /* Read from Linux kernel */
      .ip_gateway = { 0, 0, 0, 0 },       /* Read from Linux kernel */
};

/********************************** Globals ***********************************/

static os_timer_t                *main_timer = NULL;
static os_event_t                *main_events = NULL;
static uint32_t                  main_arep = UINT32_MAX;
static bool                      alarm_allowed = true;
static int                       verbosity = 0;
static struct cmd_args           arguments;
static uint16_t                  custom_modules[PNET_MAX_MODULES] = { 0 };

uint8_t                          state[PNET_MAX_MODULES][PNET_MAX_OUTPUT_LEN];


/*********************************** Callbacks ********************************/

static int app_connect_ind(
   uint32_t                arep,
   pnet_result_t           *p_result)
{
   if (verbosity > 0)
   {
      printf("Connect call-back. AREP: %u  Status codes: %d %d %d %d\n",
         arep,
         p_result->pnio_status.error_code,
         p_result->pnio_status.error_decode,
         p_result->pnio_status.error_code_1,
         p_result->pnio_status.error_code_2);
   }
   /*
    *  Handle the request on an application level.
    *  All the needed information is in the AR data structure.
    */

   return 0;
}

static int app_release_ind(
   uint32_t                arep,
   pnet_result_t           *p_result)
{
   if (verbosity > 0)
   {
      printf("Release (disconnect) call-back. AREP: %u  Status codes: %d %d %d %d\n",
         arep,
         p_result->pnio_status.error_code,
         p_result->pnio_status.error_decode,
         p_result->pnio_status.error_code_1,
         p_result->pnio_status.error_code_2);
   }

   return 0;
}

static int app_dcontrol_ind(
   uint32_t                arep,
   pnet_control_command_t  control_command,
   pnet_result_t           *p_result)
{
   if (verbosity > 0)
   {
      printf("Dcontrol call-back. AREP: %u  Command: %d  Status codes: %d %d %d %d\n",
         arep,
         control_command,
         p_result->pnio_status.error_code,
         p_result->pnio_status.error_decode,
         p_result->pnio_status.error_code_1,
         p_result->pnio_status.error_code_2);
   }

   return 0;
}

static int app_ccontrol_cnf(
   uint32_t                arep,
   pnet_result_t           *p_result)
{
   if (verbosity > 0)
   {
      printf("Ccontrol confirmation call-back. AREP: %u  Status codes: %d %d %d %d\n",
         arep,
         p_result->pnio_status.error_code,
         p_result->pnio_status.error_decode,
         p_result->pnio_status.error_code_1,
         p_result->pnio_status.error_code_2);
   }

   return 0;
}

static int app_write_ind(
   uint32_t                arep,
   uint16_t                api,
   uint16_t                slot,
   uint16_t                subslot,
   uint16_t                idx,
   uint16_t                sequence_number,
   uint16_t                write_length,
   uint8_t                 *p_write_data,
   pnet_result_t           *p_result)
{
   if (verbosity > 0)
   {
      printf("Parameter write call-back. AREP: %u API: %u Slot: %u Subslot: %u Index: %u Sequence: %u Length: %u\n",
         arep,
         api,
         slot,
         subslot,
         (unsigned)idx,
         sequence_number,
         write_length);
   }
   printf("No parameters defined.");
   return 0;
}

static int app_read_ind(
   uint32_t                arep,
   uint16_t                api,
   uint16_t                slot,
   uint16_t                subslot,
   uint16_t                idx,
   uint16_t                sequence_number,
   uint8_t                 **pp_read_data,
   uint16_t                *p_read_length,
   pnet_result_t           *p_result)
{
   if (verbosity > 0)
   {
      printf("Parameter read call-back. AREP: %u API: %u Slot: %u Subslot: %u Index: %u Sequence: %u  Max length: %u\n",
         arep,
         api,
         slot,
         subslot,
         (unsigned)idx,
         sequence_number,
         (unsigned)*p_read_length);
   }
   printf("No parameters defined.");
   return 0;
}

static int app_state_ind(
   uint32_t                arep,
   pnet_event_values_t     state)
{
   uint16_t                err_cls = 0;
   uint16_t                err_code = 0;
   uint16_t                slot = 0;

   if (state == PNET_EVENT_ABORT)
   {
      if (pnet_get_ar_error_codes(arep, &err_cls, &err_code) == 0)
      {
         if (verbosity > 0)
         {
               printf("Callback on event PNET_EVENT_ABORT. Error class: %u Error code: %u\n",
                  (unsigned)err_cls, (unsigned)err_code);
         }
      }
      else
      {
         if (verbosity > 0)
         {
               printf("Callback on event PNET_EVENT_ABORT. No error status available\n");
         }
      }
      /* Only abort AR with correct session key */
      os_event_set(main_events, EVENT_ABORT);
   }
   else if (state == PNET_EVENT_PRMEND)
   {
      if (verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_PRMEND. AREP: %u\n", arep);
      }

      /* Save the arep for later use */
      main_arep = arep;
      os_event_set(main_events, EVENT_READY_FOR_DATA);

      /* Set IOPS for DAP slot (has same numbering as the module identifiers) */
      (void)pnet_input_set_data_and_iops(APP_API, PNET_SLOT_DAP_IDENT, PNET_SUBMOD_DAP_IDENT,                    NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(APP_API, PNET_SLOT_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_IDENT,        NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(APP_API, PNET_SLOT_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_PORT_0_IDENT, NULL, 0, PNET_IOXS_GOOD);
	  /*
      (void)pnet_input_set_data_and_iops(APP_API, 1, 1, NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(APP_API, 2, 1, NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(APP_API, 3, 1, NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(APP_API, 4, 1, NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 0, 1, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 0, 0x8000, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 0, 0x8001, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 1, 1, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 2, 1, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 3, 1, PNET_IOXS_GOOD);
      (void)pnet_output_set_iocs(APP_API, 4, 1, PNET_IOXS_GOOD);
	  */
      for (slot = 0; slot < PNET_MAX_MODULES; slot++)
      {
         if (custom_modules[slot] > 0)
         {
            if (verbosity > 0)
            {
               printf("  Setting output IOCS for slot %u subslot %u\n", slot, PNET_SUBMOD_CUSTOM_IDENT);
            }
            (void)pnet_output_set_iocs(APP_API, slot, PNET_SUBMOD_CUSTOM_IDENT, PNET_IOXS_GOOD);
         }
      }

      (void)pnet_set_provider_state(true);
   }
   else if (state == PNET_EVENT_DATA)
   {
      if (verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_DATA\n");
      }
   }
   else if (state == PNET_EVENT_STARTUP)
   {
      if (verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_STARTUP\n");
      }
   }
   else if (state == PNET_EVENT_APPLRDY)
   {
      if (verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_APPLRDY\n");
      }
   }

   return 0;
}

static int app_exp_module_ind(
   uint16_t                api,
   uint16_t                slot,
   uint32_t                module_ident)
{
   int                     ret = -1;   /* Not supported in specified slot */
   uint16_t                ix;

   if (verbosity > 0)
   {
      printf("Module plug call-back\n");
   }

   /* Find module in the list of supported submodules */
   ix = 0;
   while ((ix < NELEMENTS(cfg_available_submodule_types)) &&
          (cfg_available_submodule_types[ix].module_ident_nbr != module_ident))
   {
      ix++;
   }

   if (ix < NELEMENTS(cfg_available_submodule_types))
   {
      printf("  Pull old module.    API: %u Slot: 0x%x",
         api,
         slot
      );
      if (pnet_pull_module(api, slot) != 0)
      {
         printf("    Slot was empty.\n");
      }
      else
      {
         printf("\n");
      }

      /* For now support any of the known modules in any slot */
      if (verbosity > 0)
      {
         printf("  Plug module.        API: %u Slot: 0x%x Module ID: 0x%x Index in supported modules: %u\n", api, slot, (unsigned)module_ident, ix);
      }
      ret = pnet_plug_module(api, slot, module_ident);
      if (ret != 0)
      {
         printf("Plug module failed. Ret: %u API: %u Slot: %u Module ID: 0x%x Index in list of supported modules: %u\n", ret, api, slot, (unsigned)module_ident, ix);
      }
      else
      {
         // Remember what is plugged in each slot
         if (slot < PNET_MAX_MODULES)
         {
		 	custom_modules[slot] = module_ident;
         }
         else
         {
            printf("Wrong slot number recieved: %u  It should be less than %u\n", slot, PNET_MAX_MODULES);
         }
      }

   }
   else
   {
      printf("  Module ID %08x not found. API: %u Slot: %u\n",
         (unsigned)module_ident,
         api,
         slot);
   }

   return ret;
}

static int app_exp_submodule_ind(
   uint16_t                api,
   uint16_t                slot,
   uint16_t                subslot,
   uint32_t                module_ident,
   uint32_t                submodule_ident)
{
   int                     ret = -1;
   uint16_t                ix = 0;

   if (verbosity > 0)
   {
      printf("Submodule plug call-back.\n");
   }

   /* Find it in the list of supported submodules */
   ix = 0;
   while ((ix < NELEMENTS(cfg_available_submodule_types)) &&
          ((cfg_available_submodule_types[ix].module_ident_nbr != module_ident) ||
           (cfg_available_submodule_types[ix].submodule_ident_nbr != submodule_ident)))
   {
      ix++;
   }

   if (ix < NELEMENTS(cfg_available_submodule_types))
   {
      printf("  Pull old submodule. API: %u Slot: 0x%x                   Subslot: 0x%x ",
         api,
         slot,
         subslot
      );

      if (pnet_pull_submodule(api, slot, subslot) != 0)
      {
         printf("     Subslot was empty.\n");
      } else {
         printf("\n");
      }

      if (verbosity > 0)
      {
         printf("  Plug submodule.     API: %u Slot: 0x%x Module ID: 0x%-4x Subslot: 0x%x Submodule ID: 0x%x Index in supported submodules: %u Dir: %u In: %u Out: %u bytes\n",
            api,
            slot,
            (unsigned)module_ident,
            subslot,
            (unsigned)submodule_ident,
            ix,
            cfg_available_submodule_types[ix].data_dir,
            cfg_available_submodule_types[ix].insize,
            cfg_available_submodule_types[ix].outsize
            );
      }
      ret = pnet_plug_submodule(api, slot, subslot, module_ident, submodule_ident,
         cfg_available_submodule_types[ix].data_dir,
         cfg_available_submodule_types[ix].insize,
         cfg_available_submodule_types[ix].outsize);
      if (ret != 0)
      {
         printf("  Plug submodule failed. Ret: %u API: %u Slot: %u Subslot 0x%x Module ID: 0x%x Submodule ID: 0x%x Index in list of supported modules: %u\n",
            ret,
            api,
            slot,
            subslot,
            (unsigned)module_ident,
            (unsigned)submodule_ident,
            ix);
      }
   }
   else
   {
      printf("  Submodule ID 0x%x in module ID 0x%x not found. API: %u Slot: %u Subslot %u \n",
         (unsigned)submodule_ident,
         (unsigned)module_ident,
         api,
         slot,
         subslot);
   }

   return ret;
}

static int app_new_data_status_ind(
   uint32_t                arep,
   uint32_t                crep,
   uint8_t                 changes)
{
   if (verbosity > 0)
   {
      printf("New data callback. AREP: %u  Status: 0x%02x\n", arep, changes);
   }

   return 0;
}

static int app_alarm_ind(
   uint32_t                arep,
   uint32_t                api,
   uint16_t                slot,
   uint16_t                subslot,
   uint16_t                data_len,
   uint16_t                data_usi,
   uint8_t                 *p_data)
{
   if (verbosity > 0)
   {
      printf("Alarm indicated callback. AREP: %u  API: %d  Slot: %d  Subslot: %d  Length: %d  USI: %d",
         arep,
         api,
         slot,
         subslot,
         data_len,
         data_usi);
   }
   os_event_set(main_events, EVENT_ALARM);

   return 0;
}

static int app_alarm_cnf(
   uint32_t                arep,
   pnet_pnio_status_t      *p_pnio_status)
{
   if (verbosity > 0)
   {
      printf("Alarm confirmed (by controller) callback. AREP: %u  Status code %u, %u, %u, %u\n",
         arep,
         p_pnio_status->error_code,
         p_pnio_status->error_decode,
         p_pnio_status->error_code_1,
         p_pnio_status->error_code_2);
   }
   alarm_allowed = true;

   return 0;
}

static int app_alarm_ack_cnf(
   uint32_t                arep,
   int                     res)
{
   if (verbosity > 0)
   {
      printf("Alarm ACK confirmation (from controller) callback. AREP: %u  Result: %d\n", arep, res);
   }

   return 0;
}

/************************* Utilities ******************************************/

static void main_timer_tick(
   os_timer_t              *timer,
   void                    *arg)
{
   os_event_set(main_events, EVENT_TIMER);
}

void show_usage()
{
   printf("\nProcter & Gamble high speed historian Profinet IO device.\n");
   printf("\n");
   printf("Wait for connection from IO-controller.\n");
   printf("\n");
   printf("Optional arguments:\n");
   printf("   --help       Show this help text and exit\n");
   printf("   -h           Show this help text and exit\n");
   printf("   -v           Incresase verbosity\n");
   printf("   -i INTERF    Set Ethernet interface name. Defaults to %s\n", APP_DEFAULT_ETHERNET_INTERFACE);
   printf("   -s NAME      Set station name. Defaults to %s\n", APP_DEFAULT_STATION_NAME);
}

struct cmd_args {
   char station_name[64];
   char eth_interface[64];
   int  verbosity;
};

/**
 * Parse command line arguments
 *
 * @param argc      In: Number of arguments
 * @param argv      In: Arguments
 * @return Parsed arguments
*/
struct cmd_args parse_commandline_arguments(int argc, char *argv[])
{
   // Special handling of long argument
   if (argc > 1)
   {
      if (strcmp(argv[1], "--help") == 0)
      {
         show_usage();
         exit(EXIT_CODE_ERROR);
      }
   }

   // Default values
   struct cmd_args output_arguments;
   strcpy(output_arguments.station_name, APP_DEFAULT_STATION_NAME);
   strcpy(output_arguments.eth_interface, APP_DEFAULT_ETHERNET_INTERFACE);
   output_arguments.verbosity = 0;

   int option;
   while ((option = getopt(argc, argv, "hvi:s:")) != -1) {
      switch (option) {
      case 'v':
         output_arguments.verbosity++;
         break;
      case 'i':
         strcpy(output_arguments.eth_interface, optarg);
         break;
      case 's':
         strcpy(output_arguments.station_name, optarg);
         break;
      case 'h':
      case '?':
      default:
         show_usage();
         exit(EXIT_CODE_ERROR);
      }
   }

   return output_arguments;
}

/**
 * Copy an IP address (as an integer) to a struct
 *
 * @param destination_struct  Out: destination
 * @param ip                  In: IP address
*/
void copy_ip_to_struct(pnet_cfg_ip_addr_t* destination_struct, uint32_t ip)
{
   destination_struct->a = (ip & 0xFF);
   destination_struct->b = ((ip >> 8) & 0xFF);
   destination_struct->c = ((ip >> 16) & 0xFF);
   destination_struct->d = ((ip >> 24) & 0xFF);
}

/**
 * Print an IPv4 address (without newline)
 *
 * @param ip      In: IP address
*/
void print_ip_address(uint32_t ip){
   printf("%d.%d.%d.%d",
      (ip & 0xFF),
      ((ip >> 8) & 0xFF),
      ((ip >> 16) & 0xFF),
      ((ip >> 24) & 0xFF)
   );
}

char* influx_host = "127.0.0.1";
uint16_t influx_port = 8089;
struct sockaddr_in influx_addr;
int influx_socket;
char influx_prefix[1000];
uint32_t influx_prefix_pos;
char influx_point[1000];
uint32_t influx_point_pos;
char influx_buffer[64000];
uint32_t influx_buffer_pos = 0;

void influx_init()
{
   if (verbosity > 0)
   {
      printf("Influx Init\n");
   }

   // influx_client.host = "localhost";
   // influx_client.port = 8089;

   influx_buffer_pos = 0;
   influx_prefix_pos = 0;

   influx_addr.sin_family = AF_INET;
   influx_addr.sin_addr.s_addr = inet_addr(influx_host);  // ToDo - that's optimistic, add some error handling
   influx_addr.sin_port = htons(influx_port);
   influx_socket = socket(AF_INET, SOCK_DGRAM, 0);
   if (influx_socket == -1)
      printf("ERROR creating socket!\n");
}

void influx_submit()
{
   if (verbosity > 1)
   {
      influx_buffer[influx_buffer_pos] = '\0';
      printf("Influx Submit (%d): %s\n", influx_buffer_pos, influx_buffer);
   }

   sendto(influx_socket, influx_buffer, influx_buffer_pos, 0, (struct sockaddr *)&influx_addr, sizeof(influx_addr));
   influx_buffer_pos = 0;
}

void influx_enqueue(char* measurement, char* value, int64_t timestamp)
{
   influx_point_pos = 0;
   memcpy(&influx_point[influx_point_pos], measurement, strlen(measurement));
   influx_point_pos += strlen(measurement);
   // ToDo: add tags as ",<tag_key>=<tag_value>"
   memcpy(&influx_point[influx_point_pos], " value=", 7);
   influx_point_pos += 7;
   memcpy(&influx_point[influx_point_pos], value, strlen(value));
   influx_point_pos += strlen(value);
   // ToDo: add more fields as ",<field_key>=<field_value>"
   influx_point[influx_point_pos++] = ' ';
   char ts[30];
   sprintf(ts, "%lld", timestamp);
   memcpy(&influx_point[influx_point_pos], ts, strlen(ts));
   influx_point_pos += strlen(ts);
   influx_point[influx_point_pos++] = '\n';
   if (sizeof(influx_buffer) - influx_buffer_pos < influx_prefix_pos + influx_point_pos)
      influx_submit();
   memcpy(&influx_buffer[influx_buffer_pos], influx_prefix, influx_prefix_pos);
   influx_buffer_pos += influx_prefix_pos;
   memcpy(&influx_buffer[influx_buffer_pos], influx_point, influx_point_pos);
   influx_buffer_pos += influx_point_pos;
   if (verbosity > 1)
   {
      printf("Influx Enqueue new pos: %ld\n", influx_buffer_pos);
   }
}

void influx_enqueue_bool(char* measurement, bool value, int64_t timestamp)
{
   influx_enqueue(measurement, value ? "true" : "false", timestamp);
}

void influx_enqueue_uint(char* measurement, uint64_t value, int64_t timestamp)
{
   char val[20];
   sprintf(val, "%d", value);
   influx_enqueue(measurement, val, timestamp);
}

void influx_enqueue_int(char* measurement, int64_t value, int64_t timestamp)
{
   char val[20];
   sprintf(val, "%d", value);
   influx_enqueue(measurement, val, timestamp);
}

void influx_enqueue_float(char* measurement, double value, int64_t timestamp)
{
   char val[50];
   sprintf(val, "%lf", value);
   influx_enqueue(measurement, val, timestamp);
}

void pn_main(void * arg)
{
   int            ret = -1;
   uint32_t       mask = EVENT_READY_FOR_DATA | EVENT_TIMER | EVENT_ALARM | EVENT_ABORT;
   uint32_t       flags = 0;
   uint16_t       slot = 0;
   uint8_t        ix;
   uint8_t        var_bytelen;
   char           measurement[20];
   uint8_t        outputdata[PNET_MAX_OUTPUT_LEN];
   uint8_t        outputdata_iops;
   uint16_t       outputdata_length;
   bool           outputdata_is_updated = false;

   if (verbosity > 0)
   {
      printf("Connecting to Historian\n");
   }

   influx_init();

   if (verbosity > 0)
   {
      printf("Waiting for connect request from IO-controller\n");
   }

   /* Main loop */
   for (;;)
   {
      os_event_wait(main_events, mask, &flags, OS_WAIT_FOREVER);
      if (flags & EVENT_READY_FOR_DATA)
      {
         os_event_clr(main_events, EVENT_READY_FOR_DATA); /* Re-arm */

         /* Send appl ready to profinet stack. */
         printf("Application will signal that it is ready for data.\n");
         ret = pnet_application_ready(main_arep);
         if (verbosity > 0 && ret != 0)
         {
            printf("Error returned when application telling that it is ready for data. Have you set IOCS or IOPS for all subslots?\n");
         }

         /*
          * cm_ccontrol_cnf(+/-) is indicated later (app_state_ind(DATA)), when the
          * confirmation arrives from the controller.
          */
      }
      else if (flags & EVENT_ALARM)
      {
         pnet_pnio_status_t      pnio_status = { 0,0,0,0 };

         os_event_clr(main_events, EVENT_ALARM); /* Re-arm */

         ret = pnet_alarm_send_ack(main_arep, &pnio_status);
         if (ret != 0)
         {
            printf("Error when sending alarm ACK. Error: %d\n", ret);
         }
         else if (verbosity > 0)
         {
            printf("Alarm ACK sent\n");
         }
      }
      else if (flags & EVENT_TIMER)
      {
         os_event_clr(main_events, EVENT_TIMER); /* Re-arm */

         if (main_arep != UINT32_MAX)
         {
            for (slot = 0; slot < PNET_MAX_MODULES; slot++)
            {
               if (custom_modules[slot] > 0)
               {
                  outputdata_length = sizeof(outputdata);
                  pnet_output_get_data_and_iops(APP_API, slot, PNET_SUBMOD_CUSTOM_IDENT, &outputdata_is_updated, outputdata, &outputdata_length, &outputdata_iops);

                  // ToDo : fix outputdata_is_updated in the stack
                  // if (outputdata_is_updated == true)  //TODO check IOPS
                  if (memcmp(state[slot], &outputdata, outputdata_length) != 0)
                  {
                     /* Find it in the list of supported submodules */
                     ix = 0;
                     while ((ix < NELEMENTS(cfg_available_submodule_types)) &&
                           ((cfg_available_submodule_types[ix].module_ident_nbr != custom_modules[slot]) ||
                           (cfg_available_submodule_types[ix].submodule_ident_nbr != PNET_SUBMOD_CUSTOM_IDENT)))
				         {
				            ix++;
				         }

                     // ToDo : fetch timestamp from msg
                     struct timeval tv;
                     gettimeofday(&tv, NULL);
                     int64_t timestamp = tv.tv_sec * 1000000LL + tv.tv_usec;

                     if (cfg_available_submodule_types[ix].var_bitlen == 1) {
                        for (int vi = 0; vi < cfg_available_submodule_types[ix].var_count; vi++)
                        {
                           uint8_t vby = vi >> 3;                    // ToDo: check byte order
                           uint8_t vbm = 1 << (vi - (vby * 8));      // ToDo: check bit order
                           if (state[slot][vby] & vbm != outputdata[vby] & vbm)
                           {
                              bool value = state[slot][vby] & vbm > 0;
                              if (verbosity > 0)
                                 printf("Changing [%d:%d] to %s\n", slot, vi, value ? "true" : "false");
                              // ToDo: define measurement...
                              sprintf(measurement, "b_%d_%d", slot, vi);
                              influx_enqueue_bool(measurement, value, timestamp);
                           }
                        }
                     } else {
                        uint32_t var_bytelen = cfg_available_submodule_types[ix].var_bitlen >> 3;
                        for (int vi = 0; vi < cfg_available_submodule_types[ix].var_count; vi++)
                        {
                           if (memcmp(&state[slot][vi * var_bytelen], &outputdata[vi * var_bytelen], var_bytelen) != 0)
                           {
                              if (cfg_available_submodule_types[ix].var_type == PNET_DATA_TYPE_UINT) {
                                 int64_t value = 0;
                                 if (var_bytelen == 1) {
                                    value = ((uint8_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "u8_%d_%d", slot, vi);
                                 } else if (var_bytelen == 2) {
                                    value = ((uint16_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "u16_%d_%d", slot, vi);
                                 } else if (var_bytelen == 4) {
                                    value = ((uint32_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "u32_%d_%d", slot, vi);
                                 } else if (var_bytelen == 8) {
                                    value = ((uint64_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "u64_%d_%d", slot, vi);
                                 }
                                 if (verbosity > 0)
                                    printf("Changing [%d:%d] to %lld\n", slot, vi, value);
                                 // ToDo: define measurement...
                                 influx_enqueue_uint(measurement, value, timestamp);
                              } else if (cfg_available_submodule_types[ix].var_type == PNET_DATA_TYPE_INT) {
                                 int64_t value = 0;
                                 if (var_bytelen == 1) {
                                    value = ((int8_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "i8_%d_%d", slot, vi);
                                 } else if (var_bytelen == 2) {
                                    value = ((int16_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "i16_%d_%d", slot, vi);
                                 } else if (var_bytelen == 4) {
                                    value = ((int32_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "i32_%d_%d", slot, vi);
                                 } else if (var_bytelen == 8) {
                                    value = ((int64_t *)&outputdata[vi * var_bytelen])[0];
                                    sprintf(measurement, "i64_%d_%d", slot, vi);
                                 }
                                 if (verbosity > 0)
                                    printf("Changing [%d:%d] to %lld\n", slot, vi, value);
                                 // ToDo: define measurement...
                                 influx_enqueue_int(measurement, value, timestamp);
                              } else if (cfg_available_submodule_types[ix].var_type == PNET_DATA_TYPE_FLOAT) {
                                 double value = 0;
                                 if (var_bytelen == 4) {
                                    uint32_t temp = htonl(((uint32_t *)&outputdata[vi * var_bytelen])[0]);
                                    value = *(float*)&temp;
                                    sprintf(measurement, "f32_%d_%d", slot, vi);
                                    // value = ((float *)&outputdata[vi * var_bytelen])[0];
                                 } else if (var_bytelen == 8) {
                                    uint32_t temp = htonl(((uint32_t *)&outputdata[vi * var_bytelen])[0]);
                                    value = *(double*)&temp;
                                    sprintf(measurement, "f64_%d_%d", slot, vi);
                                    // value = ((double *)&outputdata[vi * var_bytelen])[0];
                                 }
                                 if (verbosity > 0)
                                    printf("Changing [%d:%d] to %lf\n", slot, vi, value);
                                 // ToDo: define measurement...
                                 influx_enqueue_float(measurement, value, timestamp);
                              }
                           }
                        }
                     }
                     memcpy(state[slot], &outputdata, outputdata_length);
                  }
               }
            }
         }

         pnet_handle_periodic();
      }
      else if (flags & EVENT_ABORT)
      {
         /* Reset main */
         main_arep = UINT32_MAX;
         alarm_allowed = true;
         os_event_clr(main_events, EVENT_ABORT); /* Re-arm */
         if (verbosity > 0)
         {
            printf("Aborting the application\n");
         }
      }
   }
   os_timer_destroy(main_timer);
   os_event_destroy(main_events);
   printf("Ending the application\n");
}

/****************************** Main ******************************************/

int main(int argc, char *argv[])
{
   /* Parse and display command line arguments */
   arguments = parse_commandline_arguments(argc, argv);
   verbosity = arguments.verbosity;  // Global variable for use in callbacks
   printf("\n** Starting Profinet demo application **\n");
   if (verbosity > 0)
   {
      printf("Verbosity level:    %u\n", verbosity);
      printf("Ethernet interface: %s\n", arguments.eth_interface);
      printf("Station name:       %s\n", arguments.station_name);
   }

   /* Read IP, netmask and gateway */
   if (if_nametoindex(arguments.eth_interface) == 0)
   {
      printf("Error: The given Ethernet interface does not exist: %s\n", arguments.eth_interface);
      exit(EXIT_CODE_ERROR);
   }

   int fd;
   struct ifreq ifr;
   fd = socket (AF_INET, SOCK_DGRAM, 0);
   ifr.ifr_addr.sa_family = AF_INET;
   strncpy (ifr.ifr_name, arguments.eth_interface, IFNAMSIZ - 1);
   ioctl (fd, SIOCGIFADDR, &ifr);
   uint32_t ip_int = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr.s_addr;
   ioctl (fd, SIOCGIFNETMASK, &ifr);
   uint32_t netmask_int = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr.s_addr;
   uint32_t gateway_ip_int = (ip_int & 0x00FFFFFF) | 0x01000000;
	ioctl(fd, SIOCGIFHWADDR, &ifr);
   uint8_t macbuffer[6];
   memcpy(macbuffer, (uint8_t *)ifr.ifr_hwaddr.sa_data, 6);
	os_def_mac_addr(macbuffer);
   close (fd);

   if (ip_int == IP_INVALID)
   {
      printf("Error: Invalid IP address.\n");
      exit(EXIT_CODE_ERROR);
   }

   if (gateway_ip_int == IP_INVALID)
   {
      printf("Error: Invalid gateway IP address.\n");
      exit(EXIT_CODE_ERROR);
   }

   if (verbosity > 0)
   {
      printf("IP address:         ");
      print_ip_address(ip_int);
      printf("\nNetmask:            ");
      print_ip_address(netmask_int);
      printf("\nGateway:            ");
      print_ip_address(gateway_ip_int);
      printf("\nMAC address:        %02X:%02X:%02X:%02X:%02X:%02X",
         macbuffer[0],
         macbuffer[1],
         macbuffer[2],
         macbuffer[3],
         macbuffer[4],
         macbuffer[5]);
      printf("\n\n");
   }

   /* Set IP and gateway */
   strcpy(pnet_default_cfg.im_0_data.order_id, "12345");
   strcpy(pnet_default_cfg.im_0_data.im_serial_number, "00001");
   copy_ip_to_struct(&pnet_default_cfg.ip_addr, ip_int);
   copy_ip_to_struct(&pnet_default_cfg.ip_gateway, gateway_ip_int);
   copy_ip_to_struct(&pnet_default_cfg.ip_mask, netmask_int);
   strcpy(pnet_default_cfg.station_name, arguments.station_name);

   if (pnet_init(arguments.eth_interface, TICK_INTERVAL_US, &pnet_default_cfg) != 0)
   {
      printf("Failed to initialize p-net. Do you have enough Ethernet interface permission?\n");
      exit(EXIT_CODE_ERROR);
   }

   /* Initialize timer and Profinet stack */
   main_events = os_event_create();
   main_timer  = os_timer_create(TICK_INTERVAL_US, main_timer_tick, NULL, false);

   os_thread_create("pn_main", APP_PRIORITY, APP_STACKSIZE, pn_main, NULL);
   os_timer_start(main_timer);

   for(;;)
      os_usleep(APP_MAIN_SLEEPTIME_US);

   return 0;
}