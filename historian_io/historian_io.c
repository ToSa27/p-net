#ifdef PROFILING
#define _GNU_SOURCE
#include <signal.h>
#include <dlfcn.h>
#endif

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

static int app_exp_module_ind(pnet_t *net, void *arg, uint16_t api, uint16_t slot, uint32_t module_ident_number);
static int app_exp_submodule_ind(pnet_t *net, void *arg, uint16_t api, uint16_t slot, uint16_t subslot, uint32_t module_ident_number, uint32_t submodule_ident_number);
static int app_new_data_status_ind(pnet_t *net, void *arg, uint32_t arep, uint32_t crep, uint8_t changes, uint8_t data_status);
static int app_connect_ind(pnet_t *net, void *arg, uint32_t arep, pnet_result_t *p_result);
static int app_state_ind(pnet_t *net, void *arg, uint32_t arep, pnet_event_values_t state);
static int app_release_ind(pnet_t *net, void *arg, uint32_t arep, pnet_result_t *p_result);
static int app_dcontrol_ind(pnet_t *net, void *arg, uint32_t arep, pnet_control_command_t control_command, pnet_result_t *p_result);
static int app_ccontrol_cnf(pnet_t *net, void *arg, uint32_t arep, pnet_result_t *p_result);
static int app_write_ind(pnet_t *net, void *arg, uint32_t arep, uint16_t api, uint16_t slot, uint16_t subslot, uint16_t idx, uint16_t sequence_number, uint16_t write_length, uint8_t *p_write_data, pnet_result_t *p_result);
static int app_read_ind(pnet_t *net, void *arg, uint32_t arep, uint16_t api, uint16_t slot, uint16_t subslot, uint16_t idx, uint16_t sequence_number, uint8_t **pp_read_data, uint16_t *p_read_length, pnet_result_t *p_result);
static int app_alarm_cnf(pnet_t *net, void *arg, uint32_t arep, pnet_pnio_status_t *p_pnio_status);
static int app_alarm_ind(pnet_t *net, void *arg, uint32_t arep, uint32_t api, uint16_t slot, uint16_t subslot, uint16_t data_len, uint16_t data_usi, uint8_t *p_data);
static int app_alarm_ack_cnf(pnet_t *net, void *arg, uint32_t arep, int res);


/********************** Settings **********************************************/

#define EVENT_READY_FOR_DATA           BIT(0)
#define EVENT_TIMER                    BIT(1)
#define EVENT_ALARM                    BIT(2)
#define EVENT_READY_FOR_SUBMIT         BIT(8)
#define EVENT_ABORT                    BIT(15)
#define EXIT_CODE_ERROR                1

#define TICK_INTERVAL_US               500         /* org 1000 */
#define APP_PRIORITY                   15          /* org 15 */
#define APP_STACKSIZE                  4096        /* bytes */
#define APP_MAIN_SLEEPTIME_US          5*1000*1000
#define PNET_MAX_OUTPUT_LEN            1440        /* org 256 */

#define INFLUX_PRIORITY                25
#define INFLUX_STACKSIZE               4096
#define INFLUX_BUFFER_SIZE             65535
#define INFLUX_BUFFER_COUNT            5

#define IP_INVALID                     0

#define APP_DEFAULT_ETHERNET_INTERFACE "eth0"
#define APP_DEFAULT_LINE_NAME          "Line1"
#define APP_DEFAULT_CONTROLLER_NAME    "PLC1"
#define APP_DEFAULT_PROGRAM_NAME       "Program1"
#define APP_DEFAULT_INFLUX_HOST        "127.0.0.1"
#define APP_DEFAULT_INFLUX_PORT        8089
#define APP_DEFAULT_ZMQ_PORT           5555
#define APP_DEFAULT_PREFIX             ""


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
#define PNET_MOD_B01_IDENT         0x00000100
#define PNET_MOD_U08_IDENT         0x00000200
#define PNET_MOD_U16_IDENT         0x00000210
#define PNET_MOD_U32_IDENT         0x00000220
#define PNET_MOD_U64_IDENT         0x00000230
#define PNET_MOD_I08_IDENT         0x00000300
#define PNET_MOD_I16_IDENT         0x00000310
#define PNET_MOD_I32_IDENT         0x00000320
#define PNET_MOD_I64_IDENT         0x00000330
#define PNET_MOD_F32_IDENT         0x00000420
#define PNET_MOD_F64_IDENT         0x00000430
#define PNET_SUBMOD_CUSTOM_IDENT   0x00000001


/*** Lists of supported modules and submodules ********/

typedef enum pnet_data_type_values
{
   PNET_DATA_TYPE_NONE                       = 0,
   PNET_DATA_TYPE_BOOL                       = 1,
   PNET_DATA_TYPE_UINT8                      = 2,
   PNET_DATA_TYPE_UINT16                     = 3,
   PNET_DATA_TYPE_UINT32                     = 4,
   PNET_DATA_TYPE_UINT64                     = 5,
   PNET_DATA_TYPE_INT8                       = 6,
   PNET_DATA_TYPE_INT16                      = 7,
   PNET_DATA_TYPE_INT32                      = 8,
   PNET_DATA_TYPE_INT64                      = 9,
   PNET_DATA_TYPE_FLOAT32                    = 10,
   PNET_DATA_TYPE_FLOAT64                    = 11,
} pnet_data_type_values_t;

static const struct
{
   uint32_t                api;
   uint32_t                module_ident_nbr;
   uint32_t                submodule_ident_nbr;
   pnet_submodule_dir_t    data_dir;
   uint16_t                insize;      // total in byte length (here always 0)
   uint16_t                outsize;     // total out byte length
   uint32_t                var_type;    // variable type
   uint32_t                var_count;   // variable count
} cfg_available_submodule_types[] =
{
   {APP_API, PNET_MOD_DAP_IDENT, PNET_SUBMOD_DAP_IDENT, PNET_DIR_NO_IO, 0, 0, PNET_DATA_TYPE_NONE, 0},
   {APP_API, PNET_MOD_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_IDENT, PNET_DIR_NO_IO, 0, 0, PNET_DATA_TYPE_NONE, 0},
   {APP_API, PNET_MOD_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_PORT_0_IDENT, PNET_DIR_NO_IO, 0, 0, PNET_DATA_TYPE_NONE, 0},
   {APP_API, PNET_MOD_B01_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_BOOL, 2048},
   {APP_API, PNET_MOD_U08_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_UINT8, 256},
   {APP_API, PNET_MOD_U16_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_UINT16, 128},
   {APP_API, PNET_MOD_U32_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_UINT32, 64},
   {APP_API, PNET_MOD_U64_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_UINT64, 32},
   {APP_API, PNET_MOD_I08_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_INT8, 256},
   {APP_API, PNET_MOD_I16_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_INT16, 128},
   {APP_API, PNET_MOD_I32_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_INT32, 64},
   {APP_API, PNET_MOD_I64_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_INT64, 32},
   {APP_API, PNET_MOD_F32_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_FLOAT32, 64},
   {APP_API, PNET_MOD_F64_IDENT, PNET_SUBMOD_CUSTOM_IDENT, PNET_DIR_OUTPUT, 0, 256, PNET_DATA_TYPE_FLOAT64, 32},
};


/************************ App data storage ***********************************/

struct cmd_args {
   char     eth_interface[64];
   char     station_name[64];
   char     line_name[32];
   char     controller_name[32];
   char     program_name[64];
   char     prefix[16];
   int      verbosity;
};

struct influx_config
{
   char               host[64];
   uint16_t           port;
   uint32_t           max_packet_size;
   struct sockaddr_in addr;
   int                socket;
   char               fixed[1000];
   uint16_t           fixed_len;
   char               buffer[INFLUX_BUFFER_COUNT][INFLUX_BUFFER_SIZE];
   uint32_t           buffer_pos[INFLUX_BUFFER_COUNT];
   uint8_t            write_buffer;
   uint8_t            read_buffer;
};

struct zmq_config
{
   uint16_t port;
   void *   context;
   void *   pub;
};

struct statistics
{
   uint64_t           sum;
   uint32_t           count;
   uint32_t           max;
   uint32_t           alltime_max;
};

typedef struct app_data_obj
{
   os_timer_t                *main_timer;
   os_event_t                *main_events;
   uint32_t                  main_arep;
   bool                      alarm_allowed;
   struct cmd_args           arguments;
   struct statistics         stats_duration;
   struct statistics         stats_interval;
   struct statistics         stats_influx;
   struct statistics         stats_influx_enqueue;
   struct statistics         stats_zmq_enqueue;
   struct influx_config      influx;
   struct zmq_config         zmq;
   uint16_t                  custom_modules[PNET_MAX_MODULES];
   uint8_t                   state[PNET_MAX_MODULES][PNET_MAX_OUTPUT_LEN];
} app_data_t;

typedef struct app_data_and_stack_obj
{
   app_data_t           *appdata;
   pnet_t               *net;
} app_data_and_stack_t;


/************ Configuration of product ID, software version etc **************/

static pnet_cfg_t pnet_default_cfg =
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
      .cb_arg = NULL,

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
      .station_name = "",   /* Override by command line argument */
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
      .ip_addr = { 0 },                   /* Read from kernel */
      .ip_mask = { 0 },                   /* Read from kernel */
      .ip_gateway = { 0 },                /* Read from kernel */
      .eth_addr = { 0 }                   /* Read from kernel */
};


/*********************************** Callbacks ********************************/

static int app_connect_ind(
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   pnet_result_t           *p_result)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   pnet_result_t           *p_result)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   pnet_control_command_t  control_command,
   pnet_result_t           *p_result)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   pnet_result_t           *p_result)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   pnet_t                  *net,
   void                    *arg,
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
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   os_log(LOG_LEVEL_WARNING, "No parameters defined.");
   return 0;
}

static int app_read_ind(
   pnet_t                  *net,
   void                    *arg,
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
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   os_log(LOG_LEVEL_WARNING, "No parameters defined.");
   return 0;
}

static int app_state_ind(
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   pnet_event_values_t     state)
{
   uint16_t                err_cls = 0;
   uint16_t                err_code = 0;
   uint16_t                slot = 0;
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (state == PNET_EVENT_ABORT)
   {
      if (pnet_get_ar_error_codes(net, arep, &err_cls, &err_code) == 0)
      {
         if (p_appdata->arguments.verbosity > 0)
         {
               printf("Callback on event PNET_EVENT_ABORT. Error class: %u Error code: %u\n",
                  (unsigned)err_cls, (unsigned)err_code);
         }
      }
      else
      {
         if (p_appdata->arguments.verbosity > 0)
         {
               printf("Callback on event PNET_EVENT_ABORT. No error status available\n");
         }
      }
      /* Only abort AR with correct session key */
      os_event_set(p_appdata->main_events, EVENT_ABORT);
   }
   else if (state == PNET_EVENT_PRMEND)
   {
      if (p_appdata->arguments.verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_PRMEND. AREP: %u\n", arep);
      }

      /* Save the arep for later use */
      p_appdata->main_arep = arep;
      os_event_set(p_appdata->main_events, EVENT_READY_FOR_DATA);

      /* Set IOPS for DAP slot (has same numbering as the module identifiers) */
      (void)pnet_input_set_data_and_iops(net, APP_API, PNET_SLOT_DAP_IDENT, PNET_SUBMOD_DAP_IDENT,                    NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(net, APP_API, PNET_SLOT_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_IDENT,        NULL, 0, PNET_IOXS_GOOD);
      (void)pnet_input_set_data_and_iops(net, APP_API, PNET_SLOT_DAP_IDENT, PNET_SUBMOD_DAP_INTERFACE_1_PORT_0_IDENT, NULL, 0, PNET_IOXS_GOOD);

      for (slot = 0; slot < PNET_MAX_MODULES; slot++)
      {
         if (p_appdata->custom_modules[slot] > 0)
         {
            if (p_appdata->arguments.verbosity > 0)
            {
               printf("  Setting output IOCS for slot %u subslot %u\n", slot, PNET_SUBMOD_CUSTOM_IDENT);
            }
            (void)pnet_output_set_iocs(net, APP_API, slot, PNET_SUBMOD_CUSTOM_IDENT, PNET_IOXS_GOOD);
         }
      }

      (void)pnet_set_provider_state(net, true);
   }
   else if (state == PNET_EVENT_DATA)
   {
      if (p_appdata->arguments.verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_DATA\n");
      }
   }
   else if (state == PNET_EVENT_STARTUP)
   {
      if (p_appdata->arguments.verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_STARTUP\n");
      }
   }
   else if (state == PNET_EVENT_APPLRDY)
   {
      if (p_appdata->arguments.verbosity > 0)
      {
         printf("Callback on event PNET_EVENT_APPLRDY\n");
      }
   }

   return 0;
}

static int app_exp_module_ind(
   pnet_t                  *net,
   void                    *arg,
   uint16_t                api,
   uint16_t                slot,
   uint32_t                module_ident)
{
   int                     ret = -1;   /* Not supported in specified slot */
   uint16_t                ix;
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
      if (pnet_pull_module(net, api, slot) != 0)
      {
         printf("    Slot was empty.\n");
      }
      else
      {
         printf("\n");
      }

      /* For now support any of the known modules in any slot */
      if (p_appdata->arguments.verbosity > 0)
      {
         printf("  Plug module.        API: %u Slot: 0x%x Module ID: 0x%x Index in supported modules: %u\n", api, slot, (unsigned)module_ident, ix);
      }
      ret = pnet_plug_module(net, api, slot, module_ident);
      if (ret != 0)
      {
         printf("Plug module failed. Ret: %u API: %u Slot: %u Module ID: 0x%x Index in list of supported modules: %u\n", ret, api, slot, (unsigned)module_ident, ix);
      }
      else
      {
         // Remember what is plugged in each slot
         if (slot < PNET_MAX_MODULES)
         {
		 	p_appdata->custom_modules[slot] = module_ident;
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
   pnet_t                  *net,
   void                    *arg,
   uint16_t                api,
   uint16_t                slot,
   uint16_t                subslot,
   uint32_t                module_ident,
   uint32_t                submodule_ident)
{
   int                     ret = -1;
   uint16_t                ix = 0;
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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

      if (pnet_pull_submodule(net, api, slot, subslot) != 0)
      {
         printf("     Subslot was empty.\n");
      } else {
         printf("\n");
      }

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

      ret = pnet_plug_submodule(net, api, slot, subslot,
         module_ident,
         submodule_ident,
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
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   uint32_t                crep,
   uint8_t                 changes,
   uint8_t                 data_status)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
   {
      printf("New data status callback. AREP: %u  Status changes: 0x%02x  Status: 0x%02x\n", arep, changes, data_status);
   }

   return 0;
}

static int app_alarm_ind(
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   uint32_t                api,
   uint16_t                slot,
   uint16_t                subslot,
   uint16_t                data_len,
   uint16_t                data_usi,
   uint8_t                 *p_data)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
   {
      printf("Alarm indicated callback. AREP: %u  API: %d  Slot: %d  Subslot: %d  Length: %d  USI: %d",
         arep,
         api,
         slot,
         subslot,
         data_len,
         data_usi);
   }
   os_event_set(p_appdata->main_events, EVENT_ALARM);

   return 0;
}

static int app_alarm_cnf(
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   pnet_pnio_status_t      *p_pnio_status)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
   {
      printf("Alarm confirmed (by controller) callback. AREP: %u  Status code %u, %u, %u, %u\n",
         arep,
         p_pnio_status->error_code,
         p_pnio_status->error_decode,
         p_pnio_status->error_code_1,
         p_pnio_status->error_code_2);
   }
   p_appdata->alarm_allowed = true;

   return 0;
}

static int app_alarm_ack_cnf(
   pnet_t                  *net,
   void                    *arg,
   uint32_t                arep,
   int                     res)
{
   app_data_t              *p_appdata = (app_data_t*)arg;

   if (p_appdata->arguments.verbosity > 0)
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
   app_data_t              *p_appdata = (app_data_t*)arg;

   os_event_set(p_appdata->main_events, EVENT_TIMER);
}

void show_usage()
{
   printf("\nProcter & Gamble high speed historian Profinet IO device.\n");
   printf("\n");
   printf("Wait for connection from IO-controller.\n");
   printf("\n");
   printf("Optional arguments:\n");
   printf("   --help         Show this help text and exit\n");
   printf("   -h             Show this help text and exit\n");
   printf("   -v             Incresase verbosity\n");
   printf("   -i INTERF      Set Ethernet interface name. Defaults to %s\n", APP_DEFAULT_ETHERNET_INTERFACE);
   printf("   -s NAME        Set station name. Defaults to %s\n", APP_DEFAULT_STATION_NAME);
   printf("   -l NAME        Set line name. Default is %s\n", APP_DEFAULT_LINE_NAME);
   printf("   -c NAME        Set controller name. Default is %s\n", APP_DEFAULT_CONTROLLER_NAME);
   printf("   -p NAME        Set program name. Default is %s\n", APP_DEFAULT_PROGRAM_NAME);
   printf("   -d HOST        Set host for Influx database connection. Default is %s\n", APP_DEFAULT_INFLUX_HOST);
   printf("   -e PORT        Set port for Influx database connection. Set to 0 to disable Influx interface. Default is %d\n", APP_DEFAULT_INFLUX_PORT);
   printf("   -z PORT        Set port for 0MQ connection. Set to 0 to disable 0MQ interface. Default is %d\n", APP_DEFAULT_ZMQ_PORT);
   printf("   -x PREFIX      Set prefix for measurement names written to Influx. Default is %s\n", APP_DEFAULT_PREFIX);
}

/**
 * Parse command line arguments
 *
 * @param argc      In: Number of arguments
 * @param argv      In: Arguments
*/
void parse_commandline_arguments(app_data_t *p_appdata, int argc, char *argv[])
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
   struct influx_config output_influx;
   struct zmq_config output_zmq;
   strcpy(output_arguments.eth_interface, APP_DEFAULT_ETHERNET_INTERFACE);
   strcpy(output_arguments.station_name, APP_DEFAULT_STATION_NAME);
   strcpy(output_arguments.line_name, APP_DEFAULT_LINE_NAME);
   strcpy(output_arguments.controller_name, APP_DEFAULT_CONTROLLER_NAME);
   strcpy(output_arguments.program_name, APP_DEFAULT_PROGRAM_NAME);
   strcpy(output_influx.host, APP_DEFAULT_INFLUX_HOST);
   output_influx.port = APP_DEFAULT_INFLUX_PORT;
   output_zmq.port = APP_DEFAULT_ZMQ_PORT;
   strcpy(output_arguments.prefix, APP_DEFAULT_PREFIX);
   output_arguments.verbosity = 0;

   int option;
   while ((option = getopt(argc, argv, "hvi:s:l:c:p:d:e:z:x:")) != -1) {
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
      case 'l':
         strcpy(output_arguments.line_name, optarg);
         break;
      case 'c':
         strcpy(output_arguments.controller_name, optarg);
         break;
      case 'p':
         strcpy(output_arguments.program_name, optarg);
         break;
      case 'd':
         strcpy(output_influx.host, optarg);
         break;
      case 'e':
         output_influx.port = atoi(optarg);
         break;
      case 'z':
         output_zmq.port = atoi(optarg);
         break;
      case 'x':
         strcpy(output_arguments.prefix, optarg);
         break;
      case 'h':
      case '?':
      default:
         show_usage();
         exit(EXIT_CODE_ERROR);
      }
   }
   p_appdata->arguments = output_arguments;
   p_appdata->influx = output_influx;
   p_appdata->zmq = output_zmq;
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

void collect_stats(struct statistics *stats, uint64_t duration)
{
   stats->sum += duration;
   stats->count++;
   if (duration > stats->max) {
      stats->max = duration;
      if (duration > stats->alltime_max)
         stats->alltime_max = duration;
   }
}

void enqueue_raw(app_data_t *p_appdata, char* measurement, char* value, int64_t timestamp);

void persist_stats(app_data_t *p_appdata, struct statistics *stats, char *name, uint64_t timestamp)
{
   static char           measurement[100];
   static char           value_str[100];

   if (stats->count > 0) {
      sprintf(value_str, "%lf", (double)stats->sum / (double)stats->count);
      sprintf(measurement, "stats_%s_avg", name);
      enqueue_raw(p_appdata, measurement, value_str, timestamp);
   }
   sprintf(value_str, "%lf", (double)stats->count);
   sprintf(measurement, "stats_%s_count", name);
   enqueue_raw(p_appdata, measurement, value_str, timestamp);
   sprintf(value_str, "%lf", (double)stats->max);
   sprintf(measurement, "stats_%s_max", name);
   enqueue_raw(p_appdata, measurement, value_str, timestamp);
   sprintf(value_str, "%lf", (double)stats->alltime_max);
   sprintf(measurement, "stats_%s_alltimemax", name);
   enqueue_raw(p_appdata, measurement, value_str, timestamp);
   stats->sum = 0;
   stats->count = 0;
   stats->max = 0;
}

void influx_init(app_data_t *p_appdata)
{
   p_appdata->influx.write_buffer = 0;
   p_appdata->influx.read_buffer = 0;
   memset(p_appdata->influx.buffer_pos, 0, sizeof(p_appdata->influx.buffer_pos));
   p_appdata->influx.addr.sin_family = AF_INET;
   p_appdata->influx.addr.sin_addr.s_addr = inet_addr(p_appdata->influx.host);
   p_appdata->influx.addr.sin_port = htons(p_appdata->influx.port);
   p_appdata->influx.socket = socket(AF_INET, SOCK_DGRAM, 0);
   if (p_appdata->influx.socket == -1)
      printf("ERROR creating socket!\n");
   
   p_appdata->influx.fixed_len = 0;
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",ControllerName=%s", p_appdata->arguments.controller_name);
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",Global1=%s", "0");
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",Global2=%s", "0");
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",LineMode=%s", "0");
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",LineName=%s", p_appdata->arguments.line_name);
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",ProgramName=%s", p_appdata->arguments.program_name);
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",TimeShift1=%s", "0");
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",TimeShift2=%s", "0");
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",UserFilter1=%s", "Reserved1");
   p_appdata->influx.fixed_len += sprintf(&p_appdata->influx.fixed[p_appdata->influx.fixed_len], ",UserFilter2=%s", "Reserved2");
}

void influx_main(void * arg)
{
   app_data_t     *p_appdata;
   uint32_t       mask = EVENT_READY_FOR_SUBMIT;
   uint32_t       flags = 0;

   struct timeval tv;
   int64_t        timestamp_start;
   int64_t        timestamp_finish;

   p_appdata = (app_data_t*)arg;
   
   for (;;)
   {
      os_event_wait(p_appdata->main_events, mask, &flags, OS_WAIT_FOREVER);
      if (flags & EVENT_READY_FOR_SUBMIT)
      {
         os_event_clr(p_appdata->main_events, EVENT_READY_FOR_SUBMIT); /* Re-arm */
		 while (p_appdata->influx.read_buffer != p_appdata->influx.write_buffer) {
            gettimeofday(&tv, NULL);
            timestamp_start = tv.tv_sec * 1000000LL + tv.tv_usec;

			sendto(p_appdata->influx.socket, p_appdata->influx.buffer[p_appdata->influx.read_buffer], p_appdata->influx.buffer_pos[p_appdata->influx.read_buffer], 0, (struct sockaddr *)&p_appdata->influx.addr, sizeof(p_appdata->influx.addr));
            if (p_appdata->influx.read_buffer == INFLUX_BUFFER_COUNT - 1)
               p_appdata->influx.read_buffer = 0;
	        else
               (p_appdata->influx.read_buffer)++;

            gettimeofday(&tv, NULL);
            timestamp_finish = tv.tv_sec * 1000000LL + tv.tv_usec;
            collect_stats(&p_appdata->stats_influx, timestamp_finish - timestamp_start);
		 }
      }
   }
}

void influx_enqueue_core(app_data_t *p_appdata, char* point, uint32_t point_pos)
{
   if (p_appdata->influx.buffer_pos[p_appdata->influx.write_buffer] + point_pos > p_appdata->influx.max_packet_size) {
	  if (p_appdata->influx.write_buffer == INFLUX_BUFFER_COUNT - 1)
         p_appdata->influx.write_buffer = 0;
	  else
         (p_appdata->influx.write_buffer)++;
	  if (p_appdata->influx.write_buffer == p_appdata->influx.read_buffer)
         printf("Influx buffer overrun!\n");
      p_appdata->influx.buffer_pos[p_appdata->influx.write_buffer] = 0;
      os_event_set(p_appdata->main_events, EVENT_READY_FOR_SUBMIT);
   }
   memcpy(&p_appdata->influx.buffer[p_appdata->influx.write_buffer][p_appdata->influx.buffer_pos[p_appdata->influx.write_buffer]], point, point_pos);
   p_appdata->influx.buffer_pos[p_appdata->influx.write_buffer] += point_pos;
   if (p_appdata->arguments.verbosity > 1)
      printf("Influx Enqueue new pos: %ls\n", p_appdata->influx.buffer_pos);
}

void influx_enqueue_raw(app_data_t *p_appdata, char* measurement, char* value, int64_t timestamp)
{
   static char     point[1000];
   static uint32_t point_pos;
   if (p_appdata->arguments.verbosity > 1)
      printf("Influx Enqueue Raw: %s -> %s\n", measurement, value);
   point_pos = 0;
   point_pos += sprintf(&point[point_pos], "%s", measurement);
   point_pos += sprintf(&point[point_pos], ",ControllerName=%s", p_appdata->arguments.controller_name);
   point_pos += sprintf(&point[point_pos], ",ProgramName=%s", p_appdata->arguments.program_name);
   point_pos += sprintf(&point[point_pos], " value=%s", value);
   point_pos += sprintf(&point[point_pos], " %ld\n", timestamp);
   influx_enqueue_core(p_appdata, point, point_pos);
}

void influx_enqueue(app_data_t *p_appdata, char* var_type, uint8_t slot, uint16_t var_index, char* value, int64_t timestamp)
{
   static char     point[1000];
   static uint32_t point_pos;

   struct timeval tv;
   int64_t        timestamp_start;
   int64_t        timestamp_finish;

   gettimeofday(&tv, NULL);
   timestamp_start = tv.tv_sec * 1000000LL + tv.tv_usec;

   if (p_appdata->arguments.verbosity > 1)
      printf("Influx Enqueue: %s / %d / %d -> %s\n", var_type, slot, var_index, value);
   point_pos = 0;
   point_pos += sprintf(&point[point_pos], "%s%s_%hhu_%hu", p_appdata->arguments.prefix, var_type, slot, var_index);
   // point_pos += sprintf(&point[point_pos], ",ControllerName=%s", p_appdata->arguments.controller_name);
   point_pos += sprintf(&point[point_pos], ",DataType=%s", var_type);
   // point_pos += sprintf(&point[point_pos], ",Global1=%s", "0");
   // point_pos += sprintf(&point[point_pos], ",Global2=%s", "0");
   // point_pos += sprintf(&point[point_pos], ",LineMode=%s", "0");
   // point_pos += sprintf(&point[point_pos], ",LineName=%s", p_appdata->arguments.line_name);
   point_pos += sprintf(&point[point_pos], ",LineState=%s", "0");
   // point_pos += sprintf(&point[point_pos], ",ProgramName=%s", p_appdata->arguments.program_name);
   point_pos += sprintf(&point[point_pos], ",ReferenceName=%s%s_%hhu_%hu", p_appdata->arguments.prefix, var_type, slot, var_index);
   point_pos += sprintf(&point[point_pos], ",TagDescription=%s%s_%hhu_%hu", p_appdata->arguments.prefix, var_type, slot, var_index);
   // point_pos += sprintf(&point[point_pos], ",TimeShift1=%s", "0");
   // point_pos += sprintf(&point[point_pos], ",TimeShift2=%s", "0");
   // point_pos += sprintf(&point[point_pos], ",UserFilter1=%s", "Reserved1");
   // point_pos += sprintf(&point[point_pos], ",UserFilter2=%s", "Reserved2");
   memcpy(&point[point_pos], p_appdata->influx.fixed, p_appdata->influx.fixed_len);
   point_pos += p_appdata->influx.fixed_len;
   point_pos += sprintf(&point[point_pos], " value=%s", value);
   point_pos += sprintf(&point[point_pos], " %ld\n", timestamp);
   influx_enqueue_core(p_appdata, point, point_pos);

   gettimeofday(&tv, NULL);
   timestamp_finish = tv.tv_sec * 1000000LL + tv.tv_usec;
   collect_stats(&p_appdata->stats_influx_enqueue, timestamp_finish - timestamp_start);
}

void zmqp_init(app_data_t *p_appdata)
{
   p_appdata->zmq.context = zmq_ctx_new();
   p_appdata->zmq.pub = zmq_socket(p_appdata->zmq.context, ZMQ_PUB);
   zmq_bind(p_appdata->zmq.pub, "tcp:/" "/" "*:5555");  // separate strings only to avoid misinterpretation as comment in some editors
}

void zmqp_enqueue_raw(app_data_t *p_appdata, char* measurement, char* value, int64_t timestamp)
{
   static char     zmq_topic[100];
   static uint32_t zmq_topic_pos;
   static char     zmq_point[1000];
   static uint32_t zmq_point_pos;

   zmq_topic_pos = sprintf(zmq_topic, "%s", measurement);
   zmq_point_pos = 0;
   zmq_point[zmq_point_pos++] = '{';
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], "\"Measurement\":\"%s\"", measurement);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"Timestamp\"=%ld", timestamp);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"value\"=%s", value);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"ControllerName\"=\"%s\"", p_appdata->arguments.controller_name);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"ProgramName\"=\"%s\"", p_appdata->arguments.program_name);
   zmq_point[zmq_point_pos++] = '}';

   zmq_send(p_appdata->zmq.pub, zmq_topic, zmq_topic_pos, ZMQ_SNDMORE);
   zmq_send(p_appdata->zmq.pub, zmq_point, zmq_point_pos, 0);
}

void zmqp_enqueue(app_data_t *p_appdata, char* var_type, uint8_t slot, uint16_t var_index, char* value, int64_t timestamp)
{
   static char     zmq_topic[100];
   static uint32_t zmq_topic_pos;
   static char     zmq_point[1000];
   static uint32_t zmq_point_pos;

   struct timeval tv;
   int64_t        timestamp_start;
   int64_t        timestamp_finish;

   gettimeofday(&tv, NULL);
   timestamp_start = tv.tv_sec * 1000000LL + tv.tv_usec;

   zmq_topic_pos = sprintf(zmq_topic, "%s%s.%hhu.%hu", p_appdata->arguments.prefix, var_type, slot, var_index);
   zmq_point_pos = 0;
   zmq_point[zmq_point_pos++] = '{';
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], "\"Measurement\":\"%s%s_%hhu_%hu\"", p_appdata->arguments.prefix, var_type, slot, var_index);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"Timestamp\"=%ld", timestamp);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"value\"=%s", value);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"ControllerName\"=\"%s\"", p_appdata->arguments.controller_name);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"DataType\"=\"%s\"", var_type);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"Global1\"=\"%s\"", "0");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"Global2\"=\"%s\"", "0");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"LineMode\"=\"%s\"", "0");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"LineName\"=\"%s\"", p_appdata->arguments.line_name);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"LineState\"=\"%s\"", "0");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"ProgramName\"=\"%s\"", p_appdata->arguments.program_name);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"ReferenceName\"=\"%s%s_%hhu_%hu\"", p_appdata->arguments.prefix, var_type, slot, var_index);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"TagDescription\"=\"%s%s_%hhu_%hu\"", p_appdata->arguments.prefix, var_type, slot, var_index);
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"TimeShift1\"=\"%s\"", "0");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"TimeShift2\"=\"%s\"", "0");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"UserFilter1\"=\"%s\"", "Reserved1");
   zmq_point_pos += sprintf(&zmq_point[zmq_point_pos], ",\"UserFilter2\"=\"%s\"", "Reserved2");
   zmq_point[zmq_point_pos++] = '}';

   zmq_send(p_appdata->zmq.pub, zmq_topic, zmq_topic_pos, ZMQ_SNDMORE);
   zmq_send(p_appdata->zmq.pub, zmq_point, zmq_point_pos, 0);

   gettimeofday(&tv, NULL);
   timestamp_finish = tv.tv_sec * 1000000LL + tv.tv_usec;
   collect_stats(&p_appdata->stats_zmq_enqueue, timestamp_finish - timestamp_start);
}

void enqueue_init(app_data_t *p_appdata)
{
   if (p_appdata->influx.port > 0)
      influx_init(p_appdata);
   if (p_appdata->zmq.port > 0)
      zmqp_init(p_appdata);
}

void enqueue_raw(app_data_t *p_appdata, char* measurement, char* value, int64_t timestamp)
{
   if (p_appdata->influx.port > 0)
      influx_enqueue_raw(p_appdata, measurement, value, timestamp);
   if (p_appdata->zmq.port > 0)
      zmqp_enqueue_raw(p_appdata, measurement, value, timestamp);
}

void enqueue(app_data_t *p_appdata, char* var_type, uint8_t slot, uint16_t var_index, char* value, int64_t timestamp)
{
   if (p_appdata->influx.port > 0)
      influx_enqueue(p_appdata, var_type, slot, var_index, value, timestamp);
   if (p_appdata->zmq.port > 0)
      zmqp_enqueue(p_appdata, var_type, slot, var_index, value, timestamp);
}

void pn_main(void * arg)
{
   app_data_t     *p_appdata;
   app_data_and_stack_t *appdata_and_stack;
   pnet_t         *net;
   int            ret = -1;
   uint32_t       mask = EVENT_READY_FOR_DATA | EVENT_TIMER | EVENT_ALARM | EVENT_ABORT;
   uint32_t       flags = 0;
   uint16_t       slot = 0;
   uint8_t        slot_type_index;
   uint8_t        var_bytelen;
   uint16_t       var_bytepos;
   uint8_t        var_bitmask;
   uint16_t       var_index;
   char           value_str[100];
   uint8_t        outputdata[PNET_MAX_OUTPUT_LEN];
   uint8_t        outputdata_iops;
   uint16_t       outputdata_length;
   bool           outputdata_is_updated = false;

   struct timeval tv;
   int64_t        timestamp;
   int64_t        timestamp_last;
   time_t         last_stats;

   appdata_and_stack = (app_data_and_stack_t*)arg;
   p_appdata = appdata_and_stack->appdata;
   net = appdata_and_stack->net;

   printf("Connecting to Historian\n");
//   enqueue_init(p_appdata);

   printf("Waiting for connect request from IO-controller\n");

   timestamp_last = 0;
   last_stats = 0;

   /* Main loop */
   for (;;)
   {
      os_event_wait(p_appdata->main_events, mask, &flags, OS_WAIT_FOREVER);
      if (flags & EVENT_READY_FOR_DATA)
      {
         os_event_clr(p_appdata->main_events, EVENT_READY_FOR_DATA); /* Re-arm */

         /* Send appl ready to profinet stack. */
         printf("Application will signal that it is ready for data.\n");
         ret = pnet_application_ready(net, p_appdata->main_arep);
         if (ret != 0)
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

         os_event_clr(p_appdata->main_events, EVENT_ALARM); /* Re-arm */

         ret = pnet_alarm_send_ack(net, p_appdata->main_arep, &pnio_status);
         if (ret != 0)
         {
            printf("Error when sending alarm ACK. Error: %d\n", ret);
         }
         else if (p_appdata->arguments.verbosity > 0)
         {
            printf("Alarm ACK sent\n");
         }
      }
      else if (flags & EVENT_TIMER)
      {
         os_event_clr(p_appdata->main_events, EVENT_TIMER); /* Re-arm */

         if (p_appdata->main_arep != UINT32_MAX)
         {

            // ToDo : fetch timestamp from msg
            gettimeofday(&tv, NULL);
            timestamp = tv.tv_sec * 1000000LL + tv.tv_usec;

            // persist and reset stats every 10 seconds
            if (tv.tv_sec - last_stats > 10) {
			   persist_stats(p_appdata, &p_appdata->stats_interval, "interval", timestamp);
			   persist_stats(p_appdata, &p_appdata->stats_duration, "duration", timestamp);
			   persist_stats(p_appdata, &p_appdata->stats_influx, "influx_persist", timestamp);
			   persist_stats(p_appdata, &p_appdata->stats_influx_enqueue, "influx_enqueue", timestamp);
			   persist_stats(p_appdata, &p_appdata->stats_zmq_enqueue, "zmq_enqueue", timestamp);
               last_stats = tv.tv_sec;
            }

            if (timestamp_last > 0)
               collect_stats(&p_appdata->stats_interval, timestamp - timestamp_last);

            for (slot = 0; slot < PNET_MAX_MODULES; slot++)
            {
               if (p_appdata->custom_modules[slot] > 0)
               {
                  outputdata_length = sizeof(outputdata);
                  pnet_output_get_data_and_iops(net, APP_API, slot, PNET_SUBMOD_CUSTOM_IDENT, &outputdata_is_updated, outputdata, &outputdata_length, &outputdata_iops);

                  // ToDo : fix outputdata_is_updated in the stack
                  // if (outputdata_is_updated == true && outputdata_iops == PNET_IOXS_GOOD)
                  if (memcmp(p_appdata->state[slot], &outputdata, outputdata_length) != 0)
                  {
                     /* Find it in the list of supported submodules */
                     slot_type_index = 0;
                     while ((slot_type_index < NELEMENTS(cfg_available_submodule_types)) &&
                           ((cfg_available_submodule_types[slot_type_index].module_ident_nbr != p_appdata->custom_modules[slot]) ||
                           (cfg_available_submodule_types[slot_type_index].submodule_ident_nbr != PNET_SUBMOD_CUSTOM_IDENT)))
				         {
				            slot_type_index++;
				         }

                     var_bytepos = 0;
                     var_bitmask = 0b00000001;
                     var_index = 0;
                     while (var_bytepos < cfg_available_submodule_types[slot_type_index].outsize)
                     {
                        switch (cfg_available_submodule_types[slot_type_index].var_type)
                        {
                           case PNET_DATA_TYPE_BOOL:
                              if ((p_appdata->state[slot][var_bytepos] & var_bitmask) != (outputdata[var_bytepos] & var_bitmask))
                              {
                                 sprintf(value_str, "%hhu", (outputdata[var_bytepos] & var_bitmask) > 0 ? 1 : 0);
                                 enqueue(p_appdata, "b", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing b_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              if (var_bitmask == 0b10000000) {
                                 var_bytepos++;
                                 var_bitmask = 0b00000001;
                              } else {
                                 var_bitmask <<= 1;
                              }
                              break;
                           case PNET_DATA_TYPE_UINT8:
                              var_bytelen = 1;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%hhu", ((uint8_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "u8", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing u8_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_UINT16:
                              var_bytelen = 2;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%hu", ((uint16_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "u16", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing u16_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_UINT32:
                              var_bytelen = 4;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%u", ((uint32_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "u32", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing u32_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_UINT64:
                              var_bytelen = 8;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%lu", ((uint64_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "u64", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing u64_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_INT8:
                              var_bytelen = 1;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%hhi", ((int8_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "i8", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing i8_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_INT16:
                              var_bytelen = 2;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%hi", ((int16_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "i16", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing i16_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_INT32:
                              var_bytelen = 4;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%i", ((int32_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "i32", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing i32_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_INT64:
                              var_bytelen = 8;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session endianness is not little endian
                                 sprintf(value_str, "%li", ((int64_t *)&outputdata[var_bytepos])[0]);
                                 enqueue(p_appdata, "i64", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing i64_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_FLOAT32:
                              var_bytelen = 4;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session floating point representation is not IEEE
                                 uint32_t temp = htonl(((uint32_t *)&outputdata[var_bytepos])[0]);
                                 sprintf(value_str, "%f", *(float*)&temp);
                                 enqueue(p_appdata, "f32", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing f32_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           case PNET_DATA_TYPE_FLOAT64:
                              var_bytelen = 8;
                              if (memcmp(&p_appdata->state[slot][var_bytepos], &outputdata[var_bytepos], var_bytelen) != 0) {
                                 // ToDo: complain if session floating point representation is not IEEE
                                 uint64_t temp = htonl(((uint64_t *)&outputdata[var_bytepos])[0]);
                                 sprintf(value_str, "%lf", *(double*)&temp);
                                 enqueue(p_appdata, "f64", slot, var_index, value_str, timestamp);
                                 if (p_appdata->arguments.verbosity > 1)
                                    printf("Changing f64_%d_%d to %s\n", slot, var_index, value_str);
                              }
                              var_bytepos += var_bytelen;
                              break;
                           default:
                              os_log(LOG_LEVEL_WARNING, "Unknown variable type: %d", cfg_available_submodule_types[slot_type_index].var_type);
                              break;
                        }
                        var_index++;
                     }
                     memcpy(p_appdata->state[slot], &outputdata, cfg_available_submodule_types[slot_type_index].outsize);
                  }
               }
			   
               gettimeofday(&tv, NULL);
            }

            timestamp_last = timestamp;
            gettimeofday(&tv, NULL);
            timestamp = tv.tv_sec * 1000000LL + tv.tv_usec;
			
            collect_stats(&p_appdata->stats_duration, timestamp - timestamp_last);

            if (p_appdata->arguments.verbosity > 1)
               printf("rt: % 10ld\n", timestamp - timestamp_last);
            // ProfilerStop();

         }

         pnet_handle_periodic(net);
      }
      else if (flags & EVENT_ABORT)
      {
         /* Reset main */
         p_appdata->main_arep = UINT32_MAX;
         p_appdata->alarm_allowed = true;
         os_event_clr(p_appdata->main_events, EVENT_ABORT); /* Re-arm */
         if (p_appdata->arguments.verbosity > 0)
         {
            printf("Aborting the application\n");
         }
      }
   }
   os_timer_destroy(p_appdata->main_timer);
   os_event_destroy(p_appdata->main_events);
   printf("Ending the application\n");
}

/****************************** Main ******************************************/

#ifdef PROFILING
void SigIntHandler(int sig) {
    fprintf(stderr, "Exiting on SIGUSR1\n");
    void (*_mcleanup)(void);
    _mcleanup = (void (*)(void)) dlsym(RTLD_DEFAULT, "_mcleanup");
    if (_mcleanup == NULL)
        fprintf(stderr, "Unable to find gprof exit hook\n");
    else _mcleanup();
    _exit(0);
}
#endif

int main(int argc, char *argv[])
{
   pnet_t *net;
   app_data_and_stack_t appdata_and_stack;
   app_data_t appdata;
   appdata.alarm_allowed = true;
   appdata.main_arep = UINT32_MAX;
   appdata.main_events = NULL;
   appdata.main_timer = NULL;
   memset(appdata.state, 0, sizeof(appdata.state));
   memset(appdata.custom_modules, 0, sizeof(appdata.custom_modules));

#ifdef PROFILING
   signal(SIGINT, SigIntHandler);
#endif

   /* Parse and display command line arguments */
   parse_commandline_arguments(&appdata, argc, argv);

   printf("\n** Starting P&G high speed Profinet Historian IO **\n");
   printf("Number of slots:    %u (incl slot for DAP module)\n", PNET_MAX_MODULES);
   printf("Log level:          %u (DEBUG=0, ERROR=3)\n", LOG_LEVEL);
   printf("Verbosity level:    %u\n", appdata.arguments.verbosity);
   printf("Ethernet interface: %s\n", appdata.arguments.eth_interface);
   printf("Station name:       %s\n", appdata.arguments.station_name);
   printf("Line name:          %s\n", appdata.arguments.line_name);
   printf("Controller name:    %s\n", appdata.arguments.controller_name);
   printf("Program name:       %s\n", appdata.arguments.program_name);
   printf("Influx host:        %s\n", appdata.influx.host);
   printf("Influx port:        %d\n", appdata.influx.port);
   printf("0MQ port:           %d\n", appdata.zmq.port);
   printf("Prefix:             %s\n", appdata.arguments.prefix);

   /* Read IP, netmask, gateway and MAC address from operating system */
   if (if_nametoindex(appdata.arguments.eth_interface) == 0)
   {
      printf("Error: The given Ethernet interface does not exist: %s\n", appdata.arguments.eth_interface);
      exit(EXIT_CODE_ERROR);
   }

   int fd;
   struct ifreq ifr;
   fd = socket (AF_INET, SOCK_DGRAM, 0);
   ifr.ifr_addr.sa_family = AF_INET;
   strncpy (ifr.ifr_name, appdata.arguments.eth_interface, IFNAMSIZ - 1);
   ioctl(fd, SIOCGIFADDR, &ifr);
   uint32_t ip_int = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr.s_addr;
   ioctl(fd, SIOCGIFNETMASK, &ifr);
   uint32_t netmask_int = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr.s_addr;
   uint32_t gateway_ip_int = (ip_int & 0x00FFFFFF) | 0x01000000;
   ioctl(fd, SIOCGIFHWADDR, &ifr);
   uint8_t macbuffer[6];
   memcpy(macbuffer, (uint8_t *)ifr.ifr_hwaddr.sa_data, 6);
   ioctl(fd, SIOCGIFMTU, &ifr);
   uint32_t mtu = ifr.ifr_mtu;
   close(fd);

   fd = socket (AF_INET, SOCK_DGRAM, 0);
   ifr.ifr_addr.sa_family = AF_INET;
   strncpy (ifr.ifr_name, "lo", IFNAMSIZ - 1);
   ioctl(fd, SIOCGIFMTU, &ifr);
   uint32_t lo_mtu = ifr.ifr_mtu;
   close(fd);

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
   printf("\nMTU:                %d", mtu);
   printf("\nLoopback MTU:       %d", lo_mtu);
   printf("\n\n");

   /* Set IP, gateway and station name */
   strcpy(pnet_default_cfg.im_0_data.order_id, "12345");
   strcpy(pnet_default_cfg.im_0_data.im_serial_number, "00001");
   copy_ip_to_struct(&pnet_default_cfg.ip_addr, ip_int);
   copy_ip_to_struct(&pnet_default_cfg.ip_gateway, gateway_ip_int);
   copy_ip_to_struct(&pnet_default_cfg.ip_mask, netmask_int);
   memcpy(pnet_default_cfg.eth_addr.addr, macbuffer, 6);
   pnet_default_cfg.cb_arg = (void*) &appdata;
   strcpy(pnet_default_cfg.station_name, appdata.arguments.station_name);
   appdata.influx.max_packet_size = lo_mtu - 14 - 8; // 14 byte ethernet and 8 byte udp overhead


   /* Initialize profinet stack */
   net = pnet_init(appdata.arguments.eth_interface, TICK_INTERVAL_US, &pnet_default_cfg);
   if (net == NULL)
   {
      printf("Failed to initialize p-net. Do you have enough Ethernet interface permission?\n");
      exit(EXIT_CODE_ERROR);
   }
   appdata_and_stack.appdata = &appdata;
   appdata_and_stack.net = net;

   /* Initialize timer and Profinet stack */
   appdata.main_events = os_event_create();
   appdata.main_timer  = os_timer_create(TICK_INTERVAL_US, main_timer_tick, (void*)&appdata, false);
   os_thread_create("pn_main", APP_PRIORITY, APP_STACKSIZE, pn_main, (void*)&appdata_and_stack);
   os_timer_start(appdata.main_timer);

   /* Initialize influx persistance */
   enqueue_init(&appdata);
   os_thread_create("influx_main", INFLUX_PRIORITY, INFLUX_STACKSIZE, influx_main, (void*)&appdata);

   for(;;)
//   for(uint8_t i = 0; i < 12; i++) {
//      printf("main %d\n", i);
      os_usleep(APP_MAIN_SLEEPTIME_US);
//   }

   return 0;
}
