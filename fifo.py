

from main import * fifo_des
from data.path import DATA_PATH


ky_out = '\df_out.csv'
ky_in = '\df_input'
filex = DATA_PATH + ky_in

# FIFO
def fifo(FIFO_ARGS):
    fifo_df = fifo_des(filex)
    return fifo_df
