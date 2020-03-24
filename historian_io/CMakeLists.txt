#********************************************************************
#        _       _         _
#  _ __ | |_  _ | |  __ _ | |__   ___
# | '__|| __|(_)| | / _` || '_ \ / __|
# | |   | |_  _ | || (_| || |_) |\__ \
# |_|    \__|(_)|_| \__,_||_.__/ |___/
#
# http://www.rt-labs.com
# Copyright 2017 rt-labs AB, Sweden.
# See LICENSE file in the project root for full license information.
#*******************************************************************/

target_include_directories(hist_io
  PRIVATE
  ${PROFINET_SOURCE_DIR}/src
  ${PROFINET_BINARY_DIR}/src
  )

target_link_libraries (hist_io PUBLIC profinet)

install (TARGETS hist_io DESTINATION bin)
