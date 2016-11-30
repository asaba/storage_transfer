"""
Created on 22/set/2015

@author: Andrea

Questo script apre il file di impostazioni backend_servers.conf e 
si connette ai servers per copiare i file generati nella giornata

Il file configurazione ha una riga per ogni server dove sono indicati (nome del server, file locale con la chiave ssh, nome dell'utente, path del computer remoto, path di destinazione del computer locale)

Per configurare la chiave per un nuovo server vanno eseguiti sul server locale (storage)

ssh-keygen -t rsa -b 2048 -C "For data transfer from roach2"

ssh-keygen -t dsa (per le chiavi dsa)

(impostare il nome del file come /home/_user_/.ssh/id_rsa_storage e non impostare nessuna password di passphrase)
ssh-copy-id -i /home/_user_/id_rsa_storage.pub storage


Il log file ha formato storage_transfer_YYYYMMGGHHmmss.log (YYYY anno, MM mese, GG giorno, HH ore, mm minuti, ss secondi)

Aggiornato il 03/12/2015
    aggiunta la creazione della directory di salvataggio nel caso in cui non esiste
    
Aggiornamento 29/12/2015
    aggiunta la gestione delle righe di commento nel file di configurazione. Carattere di commento #

"""

import sys
import getopt
import shutil
import os
import datetime
from sendlogmail import sendmail
from fitsdbfinder import CheckFits

fits_db_command = "srt_repository_insert {db_name} {filefitsfullpath} {table}"
fits_db_command_psr = "srt_repository_insert_psr {db_name} {filefitsfullpath} {table}"
fits_extentions = [".fits", ".rf", "sf", "cf"]  # the first one is managed by fits_db_command, remain by fits_db_command_psr
transfer_enable = False
skip_fits_data = True


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hc:l:", ["conf=", "log="])
    except getopt.GetoptError:
        print 'monitor.py -c <configfile> -l <logfilepath>'
        sys.exit(2)
    content_config_file = []
    log_file_path = "."
    log_full_filename = ""
    for opt, arg in opts:
        if opt == '-h':
            print 'monitor.py -c <configfile> -l <logfilepath>'
            sys.exit()
        elif opt in ("-l", "--log"):
            log_file_path = arg
            if not check_log_file_path(log_file_path):
                sys.exit(2)
            else:
                log_full_filename = append_log_info("Start storage ", None, log_file_path)
                verbose_log_full_filename = append_log_info("Start storage ", None, log_file_path, verbose=True)
        elif opt in ("-c", "--conf"):
            config_file = arg
            content_config_file = check_config_file(config_file, log_full_filename, log_file_path)
            if content_config_file is []:
                print 'file config error'
                sys.exit(2)

    if transfer_enable:
        line_index = 0
        for server in content_config_file:
            line_index += 1
            try:
                connect_server_backend(servername=server[0], ssh_key_file=server[1], user=server[2],
                                       remotepath=server[3], localpath=server[4], log_full_filename=log_full_filename,
                                       log_file_path=log_file_path, verbose_log_full_filename=verbose_log_full_filename)
            except:
                append_log_info("Error opening server at line " + str(line_index) + " in config file",
                                log_full_filename, log_file_path)

    # Check file fits present in DB

    fitscheckerobject = CheckFits()
    line_index = 0

    for server in content_config_file:
        line_index += 1
        append_log_info("---------------------------------------------", verbose_log_full_filename, log_file_path)
        datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        log_string = datetime_prefix + " Creating the list of files fits and ar" + server[4]
        # append_log_info(command, verbose_log_full_filename, log_file_path)
        append_log_info(log_string, log_full_filename, log_file_path)
        filelist = buildfitsfileslist(server[4], ext=fits_extentions)
        datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        log_string = datetime_prefix + " List of files fits and ar created" + server[4]
        # append_log_info(command, verbose_log_full_filename, log_file_path)
        append_log_info(log_string, log_full_filename, log_file_path)

        if not skip_fits_data:
            append_log_info("---------------------------------------------", verbose_log_full_filename, log_file_path)
            datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            log_string = datetime_prefix + " Start insert new fits files in DB" + server[4]
            # append_log_info(command, verbose_log_full_filename, log_file_path)
            append_log_info(log_string, log_full_filename, log_file_path)
            for filefullpath in filelist[fits_extentions[0]]:
                try:
                    if not fitscheckerobject.check(filefullpath):
                        # file fits info not present in DB
                        command = fits_db_command.format(filefitsfullpath=filefullpath, table="tdays",
                                                         db_name="srt_storage_dev")
                        os.system(command)
                except:
                    append_log_info("Error check fits info in DB", log_full_filename, log_file_path)
            datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            log_string = datetime_prefix + " Stop insert new fits files in DB" + server[4]
            # append_log_info(command, verbose_log_full_filename, log_file_path)
            append_log_info(log_string, log_full_filename, log_file_path)

            for f in fits_extentions[1:]:
                append_log_info("---------------------------------------------", verbose_log_full_filename,
                                log_file_path)
                datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                log_string = datetime_prefix + " Start insert new {} files in DB".format(f) + server[4]
                # append_log_info(command, verbose_log_full_filename, log_file_path)
                append_log_info(log_string, log_full_filename, log_file_path)

                for filefullpath in filelist[f]:
                    try:
                        if not fitscheckerobject.check(filefullpath):
                            # file fits info not present in DB
                            command = fits_db_command_psr.format(filefitsfullpath=filefullpath, table="tdays",
                                                                 db_name="srt_storage_dev")
                            os.system(command)
                    except:
                        append_log_info("Error check fits info in DB", log_full_filename, log_file_path)
                datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                log_string = datetime_prefix + " Stop insert new {} files in DB".format(f) + server[4]
                # append_log_info(command, verbose_log_full_filename, log_file_path)
                append_log_info(log_string, log_full_filename, log_file_path)

    try:
        # copia l'ultimo file di log nel file storage_transfer_last.log
        shutil.copy2(log_full_filename, log_file_path + "/" + "storage_transfer_last.log")
    except:
        pass

    try:
        # Invia il contenuto del file di log per e-mail
        log_message = ""
        for line in open(log_file_path + "/" + "storage_transfer_last.log", "r"):
            log_message += line + "\r\n"
        sendmail(log_message)
    except:
        append_log_info("Error sending mail", log_full_filename, log_file_path)


def buildfitsfileslist(rootpath, ext):
    result = {}
    for e in ext:
        result[e] = []

    for root, dirs, files in os.walk(rootpath):
        for name in files:
            for extention in ext:
                if name[-len(extention):] == extention:
                    result[extention].append(os.path.join(root, name))
    return result


def connect_server_backend(servername, ssh_key_file, localpath, remotepath, user, log_full_filename, log_file_path,
                           verbose_log_full_filename):
    """
    crea una connessione con il server di back_end e copia i file presenti nel path
    e non presenti nel corrispondente path dello storage
    """
    # crea la directory di salvataggio
    command = "mkdir -p {localpath}".format(localpath=localpath)
    os.system(command)
    # imposta il comando di rsync
    command = 'rsync --archive --no-o --no-g -vv -r -K -e \"/usr/bin/ssh -i {ssh_key_file}\" {user}@{servername}:{remotepath} {localpath} > {log_file_path}logs/log_file_verbose_{servername}_{remotepathclean}_{now}.txt'.format(
        ssh_key_file=ssh_key_file, user=user, servername=servername, localpath=localpath, remotepath=remotepath,
        remotepathclean=remotepath.replace("/", "_"), log_file_path=log_file_path,
        now=datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    # append_log_info(command, verbose_log_full_filename, log_file_path)
    append_log_info("---------------------------------------------", verbose_log_full_filename, log_file_path)
    datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    log_string = datetime_prefix + " start transfer from " + servername + remotepath
    append_log_info(command, verbose_log_full_filename, log_file_path)
    append_log_info(log_string, log_full_filename, log_file_path)
    os.system(command)
    # esegue il comando di controllo dell'occupazione del disco
    datetime_prefix = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    log_string = datetime_prefix + " stop transfer from " + servername + remotepath
    # append_log_info(command, verbose_log_full_filename, log_file_path)
    append_log_info(log_string, log_full_filename, log_file_path)
    command = 'df -lh | grep md0 >> \"{log_file}\"'.format(log_file=verbose_log_full_filename)
    os.system(command)
    append_log_info("---------------------------------------------", verbose_log_full_filename, log_file_path)


def check_config_file(config_file_name, log_full_filename, log_file_path):
    """
    verifica che il file di configurazioni sia presente e conforme
    in caso positivo restituisce il contenuto come list of list
    in caso negativo retituisce None
    """
    try:
        if not os.path.isfile(config_file_name):
            # file not exist
            append_log_info("Config file not exists", log_full_filename, log_file_path)
            return []
        else:
            try:
                config_content = []
                with open(config_file_name) as f:
                    for line in f:
                        comment = False
                        try:
                            if line.strip()[0] == "#":
                                comment = True
                        except:
                            pass
                        if not comment:
                            line_split = line.split()
                            if len(line_split) >= 5:
                                config_content.append(line.split())
                return config_content
            except:
                append_log_info("Error file opening: " + config_file_name, log_full_filename, log_file_path)
                return []
    except:
        append_log_info("Generic error on config file: " + config_file_name, log_full_filename, log_file_path)
        return []


def check_log_file_path(log_file_path):
    """
    verifica che il path del file di log sia presente
    """
    try:
        if not os.path.exists(log_file_path):
            print("Log file path not exists")
            return False
        else:
            return True
    except:
        print("Log file path generic error: " + log_file_path)
        return False


def append_log_info(string_info, log_full_filename_x, log_file_path, verbose=False):
    """
    aggiunge una riga al file di log
    """
    if log_full_filename_x is None:
        # If log file not exist create it
        # if verbose mode is enabled create the verbose log file also
        datetime_suffix = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        if verbose:
            log_full_filename = log_file_path + "/" + "storage_transfer_verbose_" + datetime_suffix + ".log"
        else:
            log_full_filename = log_file_path + "/" + "storage_transfer_" + datetime_suffix + ".log"
    else:
        log_full_filename = log_full_filename_x
    f = open(log_full_filename, "a")
    f.write(string_info + "\n")
    f.close()
    return log_full_filename
    # try:
    #    with open(local_log_file_name, "a") as f:
    #        f.write(string_info + "\n")
    # except:
    #    print("Error append info log file " + log_full_file_name)


if __name__ == "__main__":
    if len(sys.argv) == 0:
        print 'monitor.py -c <configfile> -l <logfilepath>'
        sys.exit(2)
    else:
        main(sys.argv[1:])
