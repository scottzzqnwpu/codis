// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type MigrateTaskInfo struct {
	SlotId     int    `json:"slot_id"`
	NewGroupId int    `json:"new_group"`
	Delay      int    `json:"delay"`
	CreateAt   string `json:"create_at"`
	Percent    int    `json:"percent"`
	Status     string `json:"status"`
	Id         string `json:"-"`
}

type SlotMigrateProgress struct {
	SlotId    int `json:"slot_id"`
	FromGroup int `json:"from"`
	ToGroup   int `json:"to"`
	Remain    int `json:"remain"`
}

type MigrateInfo struct {
 	Bgsaving           			    int      `json:"bgsaving"`
 	Sync_db_file       			    int      `json:"sync_db_file"` 
 	Slot                            int      `json:"slot"`
 	Slave_ip_port                   string   `json:"slave_ip_port"`
 	Master_slot_binlog_filenum      int      `json:"master_slot_binlog_filenum"`
 	Master_slot_binlog_con_offset   int      `json:"master_slot_binlog_con_offset"`
 	Slave_slot_binlog_filenum       int      `json:"slave_slot_binlog_filenum"`
 	Slave_slot_binlog_con_offset    int      `json:"slave_slot_binlog_con_offset"`
}


func (p SlotMigrateProgress) String() string {
	return fmt.Sprintf("migrate Slot: slot_%d From: group_%d To: group_%d remain: %d keys", p.SlotId, p.FromGroup, p.ToGroup, p.Remain)
}

type MigrateTask struct {
	MigrateTaskInfo
	zkConn       zkhelper.Conn
	productName  string
	progressChan chan SlotMigrateProgress
}

func GetMigrateTask(info MigrateTaskInfo) *MigrateTask {
	return &MigrateTask{
		MigrateTaskInfo: info,
		productName:     globalEnv.ProductName(),
		zkConn:          safeZkConn,
	}
}

func (t *MigrateTask) UpdateStatus(status string) {
	t.Status = status
	b, _ := json.Marshal(t.MigrateTaskInfo)
	t.zkConn.Set(getMigrateTasksPath(t.productName)+"/"+t.Id, b, -1)
}

func (t *MigrateTask) UpdateFinish() {
	t.Status = MIGRATE_TASK_FINISHED
	t.zkConn.Delete(getMigrateTasksPath(t.productName)+"/"+t.Id, -1)
}
func (t *MigrateTask) migrateSingleSlot(slotId int, to int) error {
	// set slot status
	s, err := models.GetSlot(t.zkConn, t.productName, slotId)
	if err != nil {
		log.ErrorErrorf(err, "get slot info failed")
		return err
	}
	if s.State.Status == models.SLOT_STATUS_OFFLINE {
		log.Warnf("status is offline: %+v", s)
		return nil
	}

	from := s.GroupId
	if s.State.Status == models.SLOT_STATUS_MIGRATE {
		from = s.State.MigrateStatus.From
	}

	// make sure from group & target group exists
	exists, err := models.GroupExists(t.zkConn, t.productName, from)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		log.Errorf("src group %d not exist when migrate from %d to %d", from, from, to)
		return errors.Errorf("group %d not found", from)
	}

	exists, err = models.GroupExists(t.zkConn, t.productName, to)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.Errorf("group %d not found", to)
	}

	// cannot migrate to itself, just ignore
	if from == to {
		log.Warnf("from == to, ignore: %+v", s)
		return nil
	}

	// modify slot status
	//if err := s.SetMigrateStatus(t.zkConn, from, to); err != nil {
	//	log.ErrorErrorf(err, "set migrate status failed")
	//	return err
	//}

	err = t.Migrate(s, from, to, func(p SlotMigrateProgress) {
		// on migrate slot progress
		if p.Remain%5000 == 0 {
			log.Infof("%+v", p)
		}
	})
	if err != nil {
		log.ErrorErrorf(err, "migrate slot failed")
		return err
	}

	// migrate done, change slot status back
	s.State.Status = models.SLOT_STATUS_ONLINE
	s.State.MigrateStatus.From = models.INVALID_ID
	s.State.MigrateStatus.To = models.INVALID_ID
	s.GroupId = to
	if err := s.Update(t.zkConn); err != nil {
		log.ErrorErrorf(err, "update zk status failed, should be: %+v", s)
		return err
	}
	return nil
}

func (t *MigrateTask) run() error {
	log.Infof("migration start: %+v", t.MigrateTaskInfo)
	to := t.NewGroupId
	t.UpdateStatus(MIGRATE_TASK_MIGRATING)
	err := t.migrateSingleSlot(t.SlotId, to)
	if err != nil {
		log.ErrorErrorf(err, "migrate single slot failed")
		t.UpdateStatus(MIGRATE_TASK_ERR)
		t.rollbackPremigrate()
		return err
	}
	t.UpdateFinish()
	log.Infof("migration finished: %+v", t.MigrateTaskInfo)
	return nil
}

func (t *MigrateTask) rollbackPremigrate() {
	if s, err := models.GetSlot(t.zkConn, t.productName, t.SlotId); err == nil && s.State.Status == models.SLOT_STATUS_PRE_MIGRATE {
		s.State.Status = models.SLOT_STATUS_ONLINE
		err = s.Update(t.zkConn)
		if err != nil {
			log.Warn("rollback premigrate failed", err)
		} else {
			log.Infof("rollback slot %d from premigrate to online\n", s.Id)
		}
	}
}

var ErrGroupMasterNotFound = errors.New("group master not found")

// will block until all keys are migrated
func (task *MigrateTask) Migrate(slot *models.Slot, fromGroup, toGroup int, onProgress func(SlotMigrateProgress)) (err error) {
	groupFrom, err := models.GetGroup(task.zkConn, task.productName, fromGroup)
	if err != nil {
		return err
	}
	groupTo, err := models.GetGroup(task.zkConn, task.productName, toGroup)
	if err != nil {
		return err
	}

	fromMaster, err := groupFrom.Master(task.zkConn)
	if err != nil {
		return err
	}

	toMaster, err := groupTo.Master(task.zkConn)
	if err != nil {
		return err
	}

	if fromMaster == nil || toMaster == nil {
		return errors.Trace(ErrGroupMasterNotFound)
	}

	c, err := utils.DialTo(fromMaster.Addr, globalEnv.Password())
	if err != nil {
		return err
	}

	defer c.Close()

	to, err := utils.DialTo(toMaster.Addr, globalEnv.Password())
	if err != nil {
		return err
	}

	defer to.Close()

	//1. send toMaster migrateslot command
	_, err = utils.MigrateSlot(to, slot.Id, fromMaster.Addr)
	if err != nil{
		return err
	}
	//2. get slot migrate info from fromMaster
	//{
	//	"bgsaving": 0, 
	//	"sync_db_file": 1, 
	//	"slot": 0, 
	//	"slave_ip_port": "", 
	//	"master_slot_binlog_filenum": -1, 
	//	"master_slot_binlog_con_offset": -1, 
	//	"slave_slot_binlog_filenum": -1, 
	//	"slave_slot_binlog_con_offset": -1 
	//}
	for {
		reply, err := utils.MasterMigrateInfo(c)
		log.Infof("reply;%+v", reply)

		var info MigrateInfo
		err = json.Unmarshal([]byte(reply), &info)
		if err != nil {
			return err
		}
		if info.Bgsaving == 1{
			if info.Sync_db_file == 0 {
				if info.Slot == slot.Id {
					if info.Master_slot_binlog_filenum == info.Slave_slot_binlog_filenum&& info.Master_slot_binlog_filenum >= 0 {
						if info.Master_slot_binlog_con_offset == info.Slave_slot_binlog_con_offset && info.Master_slot_binlog_con_offset >= 0 {
							err = slot.SetPreMigrateStatus(task.zkConn, fromGroup, toGroup)
							if err != nil {
								return err
							}
							_, err = utils.FinishMigrateSlot(c, slot.Id)
							if err == nil {
								log.Infof("Finish Migrate Slot %d", slot.Id)
								break
							}

						}
					}else {
						log.Infof("Wait rsync SlotBinlog")
					}
				}else{
					log.Warnf("Slot Id Error, info_id:%d current_id:%d", info.Slot, slot.Id)
					time.Sleep(time.Duration(1000) * time.Millisecond)
				}
			}else{
				log.Infof("Dump or rsync DB")
				time.Sleep(time.Duration(1000) * time.Millisecond)
			}
		}else{
			log.Infof("Not start migrate")
			time.Sleep(time.Duration(1000) * time.Millisecond)
		}
	}
	//3. set premigrate status

	//4. when db and slotbinlog send finish, send finishmigrateslot to fromMaster

	//_, remain, err := utils.SlotsMgrtTagSlot(c, slot.Id, toMaster.Addr)
	//if err != nil {
	//	return err
	//}
	//for remain > 0 {
	//	if task.Delay > 0 {
	//		time.Sleep(time.Duration(task.Delay) * time.Millisecond)
	//	}
	//	_, remain, err = utils.SlotsMgrtTagSlot(c, slot.Id, toMaster.Addr)
	//	if remain >= 0 {
	//		onProgress(SlotMigrateProgress{
	//			SlotId:    slot.Id,
	//			FromGroup: fromGroup,
	//			ToGroup:   toGroup,
	//			Remain:    remain,
	//		})
	//	}
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (t *MigrateTask) preMigrateCheck() error {
	slots, err := models.GetMigratingSlots(safeZkConn, t.productName)

	if err != nil {
		return errors.Trace(err)
	}
	// check if there is migrating slot
	if len(slots) > 1 {
		return errors.Errorf("more than one slots are migrating, unknown error")
	}
	if len(slots) == 1 {
		slot := slots[0]
		if t.NewGroupId != slot.State.MigrateStatus.To || t.SlotId != slot.Id {
			return errors.Errorf("there is a migrating slot %+v, finish it first", slot)
		}
	}
	return nil
}
